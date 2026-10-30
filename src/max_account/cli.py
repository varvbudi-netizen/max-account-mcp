"""Команды администратора: подготовка, служба и отдельный вход владельца."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import secrets
import sys

from .account import Adapter, silence_network_logs
from .app import InstanceLock, create_app, with_saved_owner
from .config import load_settings, prepare_directories, read_service_token
from .errors import BridgeError


# Существующий ключ не меняется при повторной подготовке службы.
def initialize(settings):
    prepare_directories(settings)
    try:
        descriptor = os.open(settings.token_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        read_service_token(settings)
        return
    with os.fdopen(descriptor, "w", encoding="ascii") as destination:
        destination.write(secrets.token_urlsafe(48) + "\n")
        destination.flush()
        os.fsync(destination.fileno())


# Вход допускается только в терминале и использует блокировку основного сеанса.
async def login(settings):
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise BridgeError("AUTH_REQUIRED", "Вход выполняется владельцем в интерактивном терминале")
    prepare_directories(settings)
    lock = InstanceLock(settings.session_dir / "instance.lock")
    lock.acquire()
    account = Adapter(with_saved_owner(settings), interactive=True)
    try:
        await account.start()
        account_id = account.status().get("account_id")
        if not account_id:
            raise BridgeError("AUTH_REQUIRED", "Вход не подтверждён MAX")
        owner = settings.session_dir / "owner.json"
        temporary = owner.with_suffix(".tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as destination:
            json.dump({"account_id": str(account_id)}, destination)
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, owner)
        print("Вход выполнен. Аккаунт закреплён за этой службой.")
    finally:
        await account.close()
        lock.release()


# Номер телефона, пароль и ключ сеанса не принимаются параметрами командной строки.
def main():
    parser = argparse.ArgumentParser(description="Локальный МСП личного аккаунта MAX")
    parser.add_argument("--config", required=True, type=Path, help="Файл настроек TOML")
    parser.add_argument("command", choices=("init", "serve", "login"))
    args = parser.parse_args()
    silence_network_logs()
    if os.name != "nt":
        os.umask(0o077)
    try:
        settings = load_settings(args.config)
        if args.command == "init":
            initialize(settings)
            print("Закрытые каталоги и ключ доступа подготовлены.")
        elif args.command == "login":
            asyncio.run(login(settings))
        else:
            import uvicorn
            uvicorn.run(create_app(settings), host=settings.bind, port=settings.port, access_log=False,
                        log_level="warning", timeout_graceful_shutdown=15, proxy_headers=False)
    except BridgeError as error:
        print(f"{error.code}: {error.message}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


# Установленная команда и запуск модуля используют одну точку входа.
if __name__ == "__main__":
    raise SystemExit(main())
