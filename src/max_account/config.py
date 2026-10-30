"""Настройки службы и проверка её локальной области доступа."""

from dataclasses import dataclass
from pathlib import Path
import ipaddress
import math
import os
import stat
import tomllib

from .errors import BridgeError


# Закрытое состояние отделено от файлов, которые разрешено читать агенту.
@dataclass(frozen=True)
class Settings:
    session_dir: Path
    storage_dir: Path
    download_dir: Path
    input_roots: tuple[Path, ...]
    token_file: Path
    state_db: Path
    expected_account_id: str | None = None
    bind: str = "127.0.0.1"
    port: int = 9381
    allowed_media_hosts: tuple[str, ...] = ()
    max_file_bytes: int = 512 * 1024 * 1024
    max_photo_bytes: int = 20 * 1024 * 1024
    max_storage_bytes: int = 2 * 1024 * 1024 * 1024
    transfer_workers: int = 2
    request_timeout: float = 30.0
    upload_timeout: float = 900.0
    retention_seconds: int = 86400
    log_level: str = "INFO"

    @property
    def inbox_dir(self) -> Path:
        return self.download_dir

    @property
    def spool_dir(self) -> Path:
        return self.storage_dir


# Относительные пути вычисляются от настроек; текущая папка запуска не влияет на доступ.
def load_settings(path: str | Path) -> Settings:
    path = Path(path).resolve()
    with path.open("rb") as source:
        raw = tomllib.load(source)

    def location(name, default):
        candidate = Path(raw.get(name, default)).expanduser()
        return (path.parent / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()

    session_dir = location("session_dir", "private/session")
    storage_dir = location("storage_dir", "private/spool")
    download_dir = location("download_dir", "exchange/inbox")
    roots = raw.get("input_roots", ["exchange/outbox"])
    if not isinstance(roots, list) or not roots or any(not isinstance(item, str) for item in roots):
        raise BridgeError("CONFIG_ERROR", "Нужен список разрешённых папок отправки")
    input_roots = tuple((path.parent / Path(item).expanduser()).resolve() for item in roots)
    bind = raw.get("bind", "127.0.0.1")
    try:
        if not ipaddress.ip_address(bind).is_loopback:
            raise ValueError
    except ValueError:
        raise BridgeError("CONFIG_ERROR", "Служба должна слушать только локальный адрес") from None

    # Пересечения в любую сторону запрещены, чтобы файл сеанса не стал допустимым вложением.
    token_file = location("token_file", "private/mcp.key")
    state_db = location("state_db", "private/state.db")
    for shared in (*input_roots, download_dir):
        for protected in (session_dir, storage_dir, token_file, state_db):
            if protected == shared or protected.is_relative_to(shared) or shared.is_relative_to(protected):
                raise BridgeError("CONFIG_ERROR", "Область обмена пересекает закрытое состояние")
    hosts = raw.get("allowed_media_hosts", [])
    if not isinstance(hosts, list) or any(
        not isinstance(host, str) or not host or "/" in host or ":" in host or "*" in host
        for host in hosts
    ):
        raise BridgeError("CONFIG_ERROR", "Разрешённые хосты должны быть точными именами")
    numeric = {}
    for name, default in (
        ("port", 9381), ("max_file_bytes", 536870912), ("max_photo_bytes", 20971520),
        ("max_storage_bytes", 2147483648), ("transfer_workers", 2),
        ("request_timeout", 30), ("upload_timeout", 900), ("retention_seconds", 86400),
    ):
        value = raw.get(name, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise BridgeError("CONFIG_ERROR", f"Настройка {name} должна быть положительным числом")
        if name not in ("request_timeout", "upload_timeout") and not isinstance(value, int):
            raise BridgeError("CONFIG_ERROR", f"Настройка {name} должна быть целым числом")
        numeric[name] = value
    if numeric["port"] > 65535 or numeric["transfer_workers"] > 8:
        raise BridgeError("CONFIG_ERROR", "Порт или число передач выходят за допустимые пределы")
    return Settings(
        session_dir=session_dir, storage_dir=storage_dir, download_dir=download_dir,
        input_roots=input_roots, token_file=token_file, state_db=state_db,
        expected_account_id=str(raw["expected_account_id"]) if raw.get("expected_account_id") else None,
        bind=bind, allowed_media_hosts=tuple(host.lower().rstrip(".") for host in hosts), **numeric,
    )


# Закрытые каталоги создаются до загрузки ключа; общая папка не становится родителем секретов.
def prepare_directories(settings):
    for folder in (settings.state_db.parent, settings.token_file.parent, settings.session_dir, settings.storage_dir):
        folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Существующий каталог может принадлежать администратору; его права не меняем.
        if os.name != "nt" and folder in (settings.session_dir, settings.storage_dir, settings.state_db.parent):
            if folder.stat().st_mode & 0o077:
                raise BridgeError("CONFIG_ERROR", "Закрытый каталог доступен другим пользователям")
    for folder in (settings.download_dir, *settings.input_roots):
        folder.mkdir(parents=True, exist_ok=True, mode=0o770)
    if os.name == "posix":
        # Участник общей группы не может подменить принадлежащий службе результат скачивания.
        info = settings.download_dir.stat()
        if info.st_uid != os.geteuid() and os.geteuid() != 0:
            raise BridgeError("CONFIG_ERROR", "Каталог скачивания должен принадлежать службе")
        settings.download_dir.chmod(stat.S_IMODE(info.st_mode) | stat.S_ISVTX)


# Ключ МСП отличается от авторизации аккаунта и никогда не возвращается инструментом.
def read_service_token(settings):
    descriptor = None
    try:
        if os.name != "nt":
            parent = settings.token_file.parent.stat()
            if parent.st_uid not in (0, os.geteuid()) or parent.st_mode & 0o022:
                raise BridgeError("CONFIG_ERROR", "Каталог ключа доступен для чужой записи")
        descriptor = os.open(settings.token_file, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise BridgeError("CONFIG_ERROR", "Ключ должен быть обычным закрытым файлом")
        if os.name != "nt" and (info.st_uid != os.geteuid() or info.st_mode & 0o077):
            raise BridgeError("CONFIG_ERROR", "Ключ должен принадлежать службе и иметь права 0600")
        with os.fdopen(descriptor, "r", encoding="ascii") as source:
            descriptor = None
            token = source.read(513).strip()
    except (OSError, UnicodeError):
        raise BridgeError("CONFIG_ERROR", "Не найден локальный ключ доступа МСП") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
    if not 32 <= len(token) <= 512 or not token.isascii() or any(char.isspace() for char in token):
        raise BridgeError("CONFIG_ERROR", "Ключ доступа должен содержать не менее 32 латинских символов без пробелов")
    return token
