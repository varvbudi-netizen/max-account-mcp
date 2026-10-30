"""Закрытые снимки файлов и ограниченная передача вложений."""

import asyncio
import errno
import hashlib
import ipaddress
import mimetypes
import os
import socket
import stat
import time
import uuid
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import aiohttp

from .errors import BridgeError


CHUNK = 1024 * 1024


# Дескриптор каждого компонента фиксирует путь и не следует символическим ссылкам.
def _open_source(path, roots, session_dir):
    if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
        raise BridgeError("FILE_PATH_DENIED", "Безопасное открытие файлов доступно только на Linux")
    if ".." in Path(path).parts:
        raise BridgeError("FILE_PATH_DENIED", "Переход к родительскому каталогу запрещён")
    path = Path(os.path.abspath(path))
    session_dir = Path(os.path.abspath(session_dir))
    if path == session_dir or session_dir in path.parents:
        raise BridgeError("FILE_PATH_DENIED", "Файлы сеанса недоступны")
    for root in roots:
        root = Path(os.path.abspath(root))
        try:
            parts = path.relative_to(root).parts
        except ValueError:
            continue
        if not parts or any(part in (".", "..") for part in parts):
            break
        fd = None
        try:
            # Корень и каждый каталог открываются отдельно, без перехода по ссылкам.
            fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
            for part in root.parts[1:]:
                next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = next_fd
            for part in parts[:-1]:
                next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = next_fd
            result = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            info = os.fstat(result)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                os.close(result)
                break
            return result
        except OSError:
            break
        finally:
            if fd is not None:
                os.close(fd)
    raise BridgeError("FILE_PATH_DENIED", "Файл находится вне разрешённой области или недоступен")


# Разрешённые адреса возвращаются соединителю напрямую, без повторного DNS-запроса.
class _PublicResolver(aiohttp.abc.AbstractResolver):
    async def resolve(self, host, port=0, family=socket.AF_UNSPEC):
        try:
            entries = await asyncio.get_running_loop().getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM)
            addresses = {entry[4][0] for entry in entries}
            if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
                raise BridgeError("FILE_HOST_NOT_APPROVED", "Адрес вложения не разрешён")
            return [{"hostname": host, "host": entry[4][0], "port": port, "family": entry[0],
                     "proto": entry[2], "flags": socket.AI_NUMERICHOST} for entry in entries]
        except BridgeError:
            raise
        except Exception as exc:
            raise BridgeError("FILE_HOST_NOT_APPROVED", "Не удалось проверить адрес вложения") from exc

    async def close(self):
        pass


class MediaManager:
    def __init__(self, config, state, account):
        self.config, self.state, self.account = config, state, account
        self._tasks = set()
        self._lock = asyncio.Lock()
        self._cleanup_lock = asyncio.Lock()
        self._active = 0
        self._reserved = 0
        self._created_snapshots = set()
        self._active_parts = set()
        self.config.storage_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.config.download_dir.mkdir(parents=True, exist_ok=True, mode=0o750)

    # Учёт включает существующие файлы и одновременно выполняемые передачи.
    def _used(self):
        total = 0
        seen = set()
        for root in {self.config.storage_dir, self.config.download_dir}:
            for path in root.rglob("*"):
                if path.is_file() and not path.is_symlink() and (
                    path.suffix != ".part"
                    or root == self.config.download_dir and path.name not in self._active_parts
                ):
                    # Два имени одной жёсткой ссылки занимают место один раз.
                    info = path.stat()
                    identity = (info.st_dev, info.st_ino)
                    if info.st_ino and identity in seen:
                        continue
                    if info.st_ino:
                        seen.add(identity)
                    total += info.st_size
        return total

    async def _reserve(self, size):
        async with self._lock:
            if await asyncio.to_thread(self._used) + self._reserved + size > self.config.max_storage_bytes:
                raise BridgeError("STORAGE_FULL", "Недостаточно места для файла")
            self._reserved += size

    async def _release(self, size):
        async with self._lock:
            self._reserved -= size

    @staticmethod
    def _public(asset):
        return {key: asset[key] for key in ("file_id", "kind", "filename", "size", "sha256")}

    # При ошибке удаляются только новые снимки, которые не связаны с черновиком.
    async def prepare_attachments(self, attachments):
        result = []
        try:
            for item in attachments:
                result.append(await self._prepare_attachment(item))
            return result
        except BaseException:
            await self.remove_unclaimed_snapshots(result)
            raise

    async def remove_unclaimed_snapshots(self, assets):
        ids = [asset["file_id"] for asset in assets if asset["file_id"] in self._created_snapshots]
        async with self._cleanup_lock:
            claimed = self.state.claim_unclaimed_assets(ids)
            self._created_snapshots.difference_update(ids)
            await self._finish_deletions(claimed)

    # База защищает связанные файлы; диск проверяется по отдельному дескриптору.
    async def cleanup_expired(self):
        async with self._cleanup_lock:
            assets = self.state.claim_expired_assets(time.time() - self.config.retention_seconds, limit=50)
            return await self._finish_deletions(assets)

    # После остановки восстанавливаем только заранее записанные файлы с прежней идентичностью.
    async def recover_staged_assets(self):
        result = {"recovered": 0, "missing": 0, "preserved": 0}
        for asset in self.state.get_staged_assets():
            recovered = await self._run_file_work(self._recover_staged_asset, asset)
            if recovered is None:
                result["preserved"] += 1
            elif recovered == "missing":
                if self.state.finish_staged_asset(asset["file_id"], None):
                    result["missing"] += 1
                else:
                    result["preserved"] += 1
            elif self.state.finish_staged_asset(asset["file_id"], recovered):
                result["recovered"] += 1
            else:
                result["preserved"] += 1
        return result

    def _recover_staged_asset(self, asset):
        if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
            return None
        file_id = asset.get("file_id", "")
        path = Path(asset.get("path", ""))
        try:
            if uuid.UUID(hex=file_id).hex != file_id or path != self.config.download_dir / file_id:
                return None
        except (ValueError, TypeError, AttributeError):
            return None

        # Работаем только в защищённом каталоге и с записанным до остановки inode.
        directory = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        try:
            for part in self.config.download_dir.parts[1:]:
                next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                os.close(directory)
                directory = next_fd
            root = os.fstat(directory)
            if (root.st_uid != os.geteuid() or root.st_mode & 0o002
                    or (root.st_mode & 0o020
                        and (root.st_gid != os.getegid() or not root.st_mode & stat.S_ISVTX))):
                return None
            temporary = file_id + ".part"
            try:
                fd = os.open(file_id, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
                name = file_id
            except FileNotFoundError:
                try:
                    fd = os.open(temporary, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
                    name = temporary
                except FileNotFoundError:
                    return "missing"
                except OSError:
                    return None
            except OSError:
                return None
            with os.fdopen(fd, "rb") as reader:
                before = os.fstat(reader.fileno())
                if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                        or before.st_mode & 0o022 or before.st_nlink not in (1, 2)
                        or (before.st_dev, before.st_ino, before.st_size)
                        != (asset.get("device"), asset.get("inode"), asset.get("size"))):
                    return None
                digest, size = self._hash_reader(reader)
                after = os.fstat(reader.fileno())
                current = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if (digest != asset.get("sha256") or size != before.st_size
                        or (before.st_mtime_ns, before.st_ctime_ns) != (after.st_mtime_ns, after.st_ctime_ns)
                        or (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino)):
                    return None

                # Жёсткая ссылка создаёт конечное имя без перезаписи постороннего файла.
                if name == temporary:
                    try:
                        os.link(temporary, file_id, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
                    except FileExistsError:
                        return None
                target_info = os.stat(file_id, dir_fd=directory, follow_symlinks=False)
                if (target_info.st_dev, target_info.st_ino) != (before.st_dev, before.st_ino):
                    return None
                try:
                    temporary_info = os.stat(temporary, dir_fd=directory, follow_symlinks=False)
                except FileNotFoundError:
                    temporary_info = None
                if temporary_info is not None and (temporary_info.st_dev, temporary_info.st_ino) == (before.st_dev, before.st_ino):
                    os.unlink(temporary, dir_fd=directory)
                after = os.fstat(reader.fileno())
                target_info = os.stat(file_id, dir_fd=directory, follow_symlinks=False)
                if after.st_nlink != 1 or (target_info.st_dev, target_info.st_ino) != (before.st_dev, before.st_ino):
                    return None
            return {**asset, "status": "ready", "mtime_ns": after.st_mtime_ns, "ctime_ns": after.st_ctime_ns}
        finally:
            os.close(directory)

    async def _finish_deletions(self, assets):
        counts = {"deleted": 0, "missing": 0, "skipped": 0, "retry": 0}
        for asset in assets:
            try:
                outcome = await self._run_file_work(self._delete_managed, asset)
            except Exception:
                outcome = "retry"
            self.state.finish_asset_deletion(
                asset["file_id"], deleted=outcome in ("deleted", "missing"), preserved=outcome == "skipped"
            )
            counts[outcome] += 1
        return counts

    # Чужие пути, ссылки, заменённые файлы и изменённое содержимое не удаляются.
    def _delete_managed(self, asset):
        if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
            return "skipped"
        file_id = asset.get("file_id", "")
        try:
            if uuid.UUID(hex=file_id).hex != file_id:
                return "skipped"
        except (ValueError, AttributeError, TypeError):
            return "skipped"
        path = Path(asset.get("path", ""))
        if path.name != file_id or path.parent not in (self.config.storage_dir, self.config.download_dir):
            return "skipped"
        root = path.parent
        session = Path(os.path.abspath(self.config.session_dir))
        if root == session or session in root.parents:
            return "skipped"
        directory = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        try:
            for part in root.parts[1:]:
                next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                os.close(directory)
                directory = next_fd
            root_stat = os.fstat(directory)
            if (root_stat.st_uid != os.geteuid() or root_stat.st_mode & 0o002
                    or (root_stat.st_mode & 0o020
                        and (root_stat.st_gid != os.getegid() or not root_stat.st_mode & stat.S_ISVTX))):
                return "skipped"
            try:
                fd = os.open(file_id, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            except FileNotFoundError:
                return "missing"
            except OSError as exc:
                if exc.errno == errno.ELOOP:
                    return "skipped"
                raise
            try:
                before = os.fstat(fd)
                if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != os.geteuid()
                        or before.st_mode & 0o022
                        or asset.get("device") != before.st_dev or asset.get("inode") != before.st_ino
                        or asset.get("mtime_ns") != before.st_mtime_ns or asset.get("ctime_ns") != before.st_ctime_ns
                        or asset.get("size") != before.st_size):
                    return "skipped"
                with os.fdopen(fd, "rb", closefd=False) as reader:
                    digest, size = self._hash_reader(reader)
                after = os.fstat(fd)
                current = os.stat(file_id, dir_fd=directory, follow_symlinks=False)
                if (digest != asset.get("sha256") or size != before.st_size
                        or (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns)
                        != (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns)
                        or (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino)):
                    return "skipped"
                os.unlink(file_id, dir_fd=directory)
                return "deleted"
            finally:
                os.close(fd)
        finally:
            os.close(directory)

    # Копия создаётся из одного дескриптора и проверяется до и после чтения.
    async def _prepare_attachment(self, item):
            kind = item.get("kind", "document")
            if kind not in ("document", "photo") or bool(item.get("path")) == bool(item.get("file_id")):
                raise BridgeError("MEDIA_UNSUPPORTED", "Укажите файл и поддерживаемый вид вложения")
            source = self.state.get_asset(file_id=item["file_id"]) if item.get("file_id") else None
            path = source.get("path") if source else item.get("path")
            if not path:
                raise BridgeError("FILE_EXPIRED", "Файл больше недоступен")
            if source and Path(path).parent == self.config.download_dir and time.time() >= source["created_at"] + self.config.retention_seconds:
                raise BridgeError("FILE_EXPIRED", "Срок хранения файла истёк")
            roots = (self.config.download_dir, self.config.storage_dir) if source else self.config.input_roots
            fd = _open_source(path, roots, self.config.session_dir)
            info = os.fstat(fd)
            limit = min(self.config.max_file_bytes, self.config.max_photo_bytes) if kind == "photo" else self.config.max_file_bytes
            if info.st_size > limit:
                os.close(fd)
                raise BridgeError("FILE_TOO_LARGE", "Файл превышает допустимый размер")
            if info.st_size == 0:
                os.close(fd)
                raise BridgeError("MEDIA_UNSUPPORTED", "Отправка пустого файла пока не поддерживается")
            try:
                await self._reserve(info.st_size)
            except BaseException:
                os.close(fd)
                raise
            file_id = uuid.uuid4().hex
            target = self.config.storage_dir / file_id
            part = target.with_suffix(".part")
            saved = False
            try:
                copy_task = asyncio.create_task(asyncio.to_thread(self._copy_snapshot, fd, part, info, limit))
                try:
                    digest, written = await asyncio.shield(copy_task)
                except asyncio.CancelledError:
                    await copy_task
                    raise
                if source and (source["sha256"] != digest or source["size"] != written):
                    raise BridgeError("FILE_CHANGED", "Сохранённый файл изменился")
                os.chmod(part, 0o400)
                os.replace(part, target)
                identity = target.stat()
                asset = {"file_id": file_id, "path": str(target), "kind": kind,
                         "filename": source["filename"] if source else Path(path).name,
                         "size": written, "sha256": digest, "status": "ready",
                         "created_at": time.time(), "unsafe": bool(source and source.get("unsafe")),
                         "device": identity.st_dev, "inode": identity.st_ino,
                         "mtime_ns": identity.st_mtime_ns, "ctime_ns": identity.st_ctime_ns}
                self.state.save_asset(asset)
                saved = True
                self._created_snapshots.add(file_id)
                return self._public(asset)
            finally:
                part.unlink(missing_ok=True)
                if not saved:
                    target.unlink(missing_ok=True)
                await self._release(info.st_size)

    @staticmethod
    def _copy_snapshot(fd, part, info, limit):
        digest = hashlib.sha256()
        written = 0
        with os.fdopen(fd, "rb") as reader, open(part, "xb") as writer:
            os.chmod(part, 0o600)
            while chunk := reader.read(CHUNK):
                written += len(chunk)
                if written > info.st_size or written > limit:
                    raise BridgeError("FILE_CHANGED", "Исходный файл изменился")
                digest.update(chunk)
                writer.write(chunk)
            after = os.fstat(reader.fileno())
            if (info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns) or written != info.st_size:
                raise BridgeError("FILE_CHANGED", "Исходный файл изменился")
        return digest.hexdigest(), written

    # Отмена дожидается завершения операции с дескриптором, чтобы не закрыть его в другом потоке.
    @staticmethod
    async def _run_file_work(function, *args):
        task = asyncio.create_task(asyncio.to_thread(function, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    def _check_url(self, url):
        try:
            parsed = urlsplit(url)
            host = (parsed.hostname or "").lower().rstrip(".")
            if parsed.scheme != "https" or not host or parsed.username or parsed.password or parsed.port not in (None, 443):
                raise ValueError()
            if host not in {name.lower().rstrip(".") for name in self.config.allowed_media_hosts}:
                raise ValueError()
            try:
                ipaddress.ip_address(host)
            except ValueError:
                return
        except (ValueError, TypeError):
            pass
        raise BridgeError("FILE_HOST_NOT_APPROVED", "Адрес вложения не разрешён")

    # Клиент не использует прокси окружения и не следует перенаправлениям автоматически.
    async def _request(self, method, url, **kwargs):
        connector = aiohttp.TCPConnector(resolver=_PublicResolver(), use_dns_cache=False)
        timeout = aiohttp.ClientTimeout(total=self.config.upload_timeout if method != "GET" else self.config.request_timeout)
        session = aiohttp.ClientSession(connector=connector, trust_env=False, timeout=timeout, auto_decompress=False)
        try:
            for _ in range(4):
                self._check_url(url)
                response = await session.request(method, url, allow_redirects=False, proxy=None, **kwargs)
                if response.status not in (301, 302, 303, 307, 308):
                    return session, response
                location = response.headers.get("Location")
                response.release()
                if method != "GET" or not location:
                    raise BridgeError("FILE_HOST_NOT_APPROVED", "Перенаправление вложения отклонено")
                url = urljoin(url, location)
            raise BridgeError("FILE_HOST_NOT_APPROVED", "Слишком много перенаправлений вложения")
        except BaseException:
            await session.close()
            raise

    async def start_download(self, request_key, chat_id, message_id, attachment_id, batch_id=None):
        fingerprint = [chat_id, message_id, attachment_id, batch_id]
        async with self._lock:
            old = self.state.get_transfer(request_key=request_key)
            if old:
                self.state.claim_transfer(request_key, fingerprint, batch_id)
                return await self.get_transfer_status(old["transfer_id"])
            if self._active >= self.config.transfer_workers:
                raise BridgeError("BUSY", "Все передачи заняты", retryable=True, retry_after_seconds=5)
            transfer, created = self.state.claim_transfer(request_key, fingerprint, batch_id)
            if not created:
                return transfer
            self._active += 1
            task = asyncio.create_task(self._download(transfer, chat_id, message_id, attachment_id))
            self._tasks.add(task)
            task.add_done_callback(self._finish_task)
            return transfer

    # Исключение фоновой передачи не должно раскрывать адрес вложения в журнале среды.
    def _finish_task(self, task):
        self._tasks.discard(task)
        if not task.cancelled():
            task.exception()

    async def get_transfer_status(self, transfer_id):
        transfer = self.state.get_transfer(transfer_id=transfer_id)
        if not transfer:
            raise BridgeError("FILE_EXPIRED", "Передача не найдена")
        if transfer.get("status") == "ready":
            asset = self.state.get_asset(file_id=transfer["result"]["file_id"])
            if (not asset or time.time() >= asset["created_at"] + self.config.retention_seconds
                    or not Path(asset["path"]).is_file() or await self._run_file_work(self._hash, Path(asset["path"])) != asset["sha256"]):
                raise BridgeError("FILE_EXPIRED", "Скачанный файл больше недоступен")
        return transfer

    @staticmethod
    def _hash(path):
        digest = hashlib.sha256()
        with path.open("rb") as reader:
            while chunk := reader.read(CHUNK):
                digest.update(chunk)
        return digest.hexdigest()

    async def _download(self, transfer, chat_id, message_id, attachment_id):
        transfer_id = transfer["transfer_id"]
        file_id = uuid.uuid4().hex
        part = self.config.download_dir / (file_id + ".part")
        self._active_parts.add(part.name)
        reserved = 0
        staged = False
        try:
            self.state.update_transfer(transfer_id, {"status": "downloading"})
            attachment = await self.account.resolve_attachment(chat_id, message_id, attachment_id)
            session, response = await self._request("GET", attachment["url"])
            try:
                if response.status != 200:
                    raise BridgeError("UPSTREAM_ERROR", "Не удалось скачать вложение")
                expected = response.content_length
                if expected is not None and expected > self.config.max_file_bytes:
                    raise BridgeError("FILE_TOO_LARGE", "Файл превышает допустимый размер")
                digest = hashlib.sha256()
                size = 0
                with open(part, "xb") as writer:
                    while chunk := await response.content.read(CHUNK):
                        size += len(chunk)
                        if size > self.config.max_file_bytes:
                            raise BridgeError("FILE_TOO_LARGE", "Файл превышает допустимый размер")
                        await self._reserve(len(chunk))
                        reserved += len(chunk)
                        await self._run_file_work(writer.write, chunk)
                        digest.update(chunk)
                        self.state.update_transfer(transfer_id, {"progress_bytes": size})
                if expected is not None and size != expected:
                    raise BridgeError("UPSTREAM_ERROR", "Размер полученного файла не совпадает")
            finally:
                response.release()
                await session.close()
            target = self.config.download_dir / file_id
            os.chmod(part, 0o640)
            identity = part.stat()
            asset = {"file_id": file_id, "path": str(target), "kind": attachment.get("kind", "document"),
                     "filename": attachment.get("filename") or "вложение", "size": size,
                     "sha256": digest.hexdigest(), "status": "staging", "unsafe": bool(attachment.get("unsafe")),
                     "created_at": time.time(), "batch_id": transfer.get("batch_id"),
                     "request_key": transfer.get("request_key"), "device": identity.st_dev, "inode": identity.st_ino,
                     "mtime_ns": identity.st_mtime_ns, "ctime_ns": identity.st_ctime_ns}
            # Запись появляется раньше переименования: после сбоя файл можно связать с операцией.
            staged = True
            try:
                self.state.save_asset(asset)
            except BaseException:
                # Ошибка могла возникнуть после фиксации; удаляем часть только если записи точно нет.
                try:
                    staged = any(item["file_id"] == file_id for item in self.state.get_staged_assets())
                except Exception:
                    pass
                raise
            # Конечное имя создаётся без перезаписи уже существующего файла.
            os.link(part, target)
            part.unlink()
            identity = target.lstat()
            if (not stat.S_ISREG(identity.st_mode) or identity.st_nlink != 1
                    or (identity.st_dev, identity.st_ino, identity.st_size)
                    != (asset["device"], asset["inode"], asset["size"])):
                raise BridgeError("FILE_CHANGED", "Скачанный файл изменился")
            asset.update(status="ready", mtime_ns=identity.st_mtime_ns, ctime_ns=identity.st_ctime_ns)
            if not self.state.finish_staged_asset(file_id, asset):
                raise BridgeError("UPSTREAM_ERROR", "Не удалось сохранить скачанный файл")
            public = {**self._public(asset), "path": str(target), "unsafe": asset["unsafe"],
                      "expires_at": asset["created_at"] + self.config.retention_seconds}
            self.state.update_transfer(transfer_id, {"status": "ready", "result": public})
        except asyncio.CancelledError:
            self.state.update_transfer(transfer_id, {"status": "failed", "error": {"code": "UPSTREAM_ERROR", "message": "Передача прервана"}})
            raise
        except Exception as exc:
            error = exc if isinstance(exc, BridgeError) else BridgeError("UPSTREAM_ERROR", "Не удалось скачать вложение")
            self.state.update_transfer(transfer_id, {"status": "failed", "error": error.to_dict()})
        finally:
            if not staged:
                part.unlink(missing_ok=True)
            self._active_parts.discard(part.name)
            await self._release(reserved)
            async with self._lock:
                self._active -= 1

    async def upload_prepared(self, assets):
        result = []
        for public in assets:
            asset = self.state.get_asset(file_id=public["file_id"])
            if not asset or any(asset.get(key) != public.get(key) for key in ("kind", "filename", "size", "sha256")):
                raise BridgeError("FILE_CHANGED", "Подготовленный файл изменился")
            fd = _open_source(asset["path"], (self.config.storage_dir,), self.config.session_dir)
            prepared = None
            try:
                # Проверка и загрузка используют один и тот же открытый файл.
                with os.fdopen(fd, "rb") as reader:
                    digest, length = await self._run_file_work(self._hash_reader, reader)
                    if digest != asset["sha256"] or length != asset["size"]:
                        raise BridgeError("FILE_CHANGED", "Подготовленный файл изменился")
                    reader.seek(0)
                    prepared = await self.account.prepare_upload(asset)
                    data = reader
                    if prepared.get("form_field"):
                        data = aiohttp.FormData()
                        content_type = mimetypes.guess_type(asset["filename"])[0] or "application/octet-stream"
                        data.add_field(prepared["form_field"], reader, filename=asset["filename"], content_type=content_type)
                    session, response = await self._request(prepared["method"], prepared["url"], headers=prepared.get("headers"), data=data)
                    try:
                        body = bytearray()
                        async for chunk in response.content.iter_chunked(64 * 1024):
                            body.extend(chunk)
                            if len(body) > 1024 * 1024:
                                break
                        if len(body) > 1024 * 1024:
                            raise BridgeError("UPSTREAM_ERROR", "Ответ загрузки слишком велик")
                        result.append(await self.account.finish_upload(prepared, bytes(body), response.status, dict(response.headers)))
                        prepared = None
                    finally:
                        response.release()
                        await session.close()
            except BridgeError:
                raise
            except Exception as exc:
                raise BridgeError("UPSTREAM_ERROR", "Не удалось загрузить вложение") from exc
            finally:
                if prepared is not None:
                    await self.account.cancel_upload(prepared)
        return result

    @staticmethod
    def _hash_reader(reader):
        digest = hashlib.sha256()
        while chunk := reader.read(CHUNK):
            digest.update(chunk)
        return digest.hexdigest(), reader.tell()

    async def shutdown(self):
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
