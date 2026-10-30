"""Проверки границ файловой области и повторов передач."""

import asyncio
import gc
import hashlib
import os
import socket
import ssl
import subprocess
import threading
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp import web

from max_account.errors import BridgeError
from max_account import media as media_module
from max_account.media import MediaManager, _PublicResolver
from max_account.state import StateStore


def manager(tmp_path, account=None):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    config = SimpleNamespace(
        input_roots=(incoming,), storage_dir=tmp_path / "private", download_dir=tmp_path / "downloads",
        session_dir=tmp_path / "session", max_file_bytes=32, max_photo_bytes=16, max_storage_bytes=64,
        allowed_media_hosts=("media.example",), transfer_workers=2, upload_timeout=5,
        request_timeout=5, retention_seconds=100,
    )
    return MediaManager(config, StateStore(tmp_path / "state.db"), account), incoming


# На Windows проверяется взаимодействие с базой; защищённое удаление отдельно проверяется на Linux.
def allow_test_deletion_on_windows(media, monkeypatch):
    if os.name != "nt":
        return
    def delete(asset):
        path = Path(asset["path"])
        if not path.exists():
            return "missing"
        info = path.stat()
        if (info.st_dev != asset.get("device") or info.st_ino != asset.get("inode")
                or info.st_size != asset["size"] or media._hash(path) != asset["sha256"]):
            return "skipped"
        os.chmod(path, 0o600)
        path.unlink()
        return "deleted"
    monkeypatch.setattr(media, "_delete_managed", delete)


# Снимок независим от исходного файла; изменение самого снимка обнаруживается до сети.
@pytest.mark.skipif(os.name != "posix", reason="Защищённое открытие реализовано для Linux")
def test_snapshot_and_changed_snapshot(tmp_path):
    media, incoming = manager(tmp_path)
    original = incoming / "document.txt"
    original.write_bytes(b"first")
    asset = asyncio.run(media.prepare_attachments([{"path": str(original), "kind": "document"}]))[0]
    original.write_bytes(b"second")
    stored = media.state.get_asset(file_id=asset["file_id"])
    assert open(stored["path"], "rb").read() == b"first"
    assert "path" not in asset
    os.chmod(stored["path"], 0o600)
    with open(stored["path"], "wb") as target:
        target.write(b"other")
    with pytest.raises(BridgeError) as error:
        asyncio.run(media.upload_prepared([asset]))
    assert error.value.code == "FILE_CHANGED"


# Ссылки, обход через родителя и специальные файлы не становятся вложениями.
@pytest.mark.skipif(os.name != "posix", reason="Защищённое открытие реализовано для Linux")
def test_paths_and_storage_limit(tmp_path):
    media, incoming = manager(tmp_path)
    outside = tmp_path / "secret"
    outside.write_bytes(b"secret")
    (incoming / "link").symlink_to(outside)
    os.mkfifo(incoming / "pipe")
    for path in (incoming / "link", incoming / "pipe", incoming / ".." / "secret"):
        with pytest.raises(BridgeError) as error:
            asyncio.run(media.prepare_attachments([{"path": str(path)}]))
        assert error.value.code == "FILE_PATH_DENIED"
    (incoming / "large").write_bytes(b"x" * 33)
    with pytest.raises(BridgeError) as error:
        asyncio.run(media.prepare_attachments([{"path": str(incoming / "large")}]))
    assert error.value.code == "FILE_TOO_LARGE"


# DNS не допускает смешанный ответ с закрытым адресом, даже если есть публичный.
def test_resolver_rejects_mixed_dns(monkeypatch):
    async def scenario():
        loop = asyncio.get_running_loop()
        async def fake_lookup(*args, **kwargs):
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443)),
                    (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]
        monkeypatch.setattr(loop, "getaddrinfo", fake_lookup)
        with pytest.raises(BridgeError) as error:
            await _PublicResolver().resolve("media.example", 443)
        assert error.value.code == "FILE_HOST_NOT_APPROVED"
    asyncio.run(scenario())


# Один ключ всегда связан с одним вложением, даже при одновременных запросах.
def test_duplicate_download(tmp_path):
    class Account:
        async def resolve_attachment(self, *args):
            await asyncio.Event().wait()
    async def scenario():
        media, _ = manager(tmp_path, Account())
        first, second = await asyncio.gather(
            media.start_download("one", 1, 2, 0), media.start_download("one", 1, 2, 0),
        )
        assert first["transfer_id"] == second["transfer_id"]
        with pytest.raises(BridgeError) as error:
            await media.start_download("one", 1, 3, 0)
        assert error.value.code == "REQUEST_CONFLICT"
        await media.shutdown()
    asyncio.run(scenario())


# Даже при сбое записи ошибки адрес вложения не попадает в общий журнал событий.
def test_download_failure_does_not_log_secret_url(tmp_path, monkeypatch):
    secret_url = "https://media.example/secret-token-marker"

    class Account:
        async def resolve_attachment(self, *args):
            raise RuntimeError(secret_url)

    async def scenario():
        media, _ = manager(tmp_path, Account())
        original_update = media.state.update_transfer
        def update(transfer_id, fields):
            if fields.get("status") == "failed":
                raise OSError("Ошибка записи состояния")
            return original_update(transfer_id, fields)
        monkeypatch.setattr(media.state, "update_transfer", update)
        loop = asyncio.get_running_loop()
        contexts = []
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda _, context: contexts.append(context))
        try:
            await media.start_download("failure", 1, 2, 0)
            while media._tasks:
                await asyncio.sleep(0)
            gc.collect()
            await asyncio.sleep(0)
            assert contexts == []
        finally:
            loop.set_exception_handler(previous)
    asyncio.run(scenario())


def test_url_policy(tmp_path):
    media, _ = manager(tmp_path)
    media._check_url("https://media.example/file?secret=1")
    for url in ("http://media.example/file", "https://evil.example/file", "https://127.0.0.1/file",
                "https://user:secret@media.example/file"):
        with pytest.raises(BridgeError) as error:
            media._check_url(url)
        assert error.value.code == "FILE_HOST_NOT_APPROVED"
        assert "secret" not in str(error.value)


# Лимит проверяется по полученным байтам, даже когда сервер не сообщает длину.
def test_download_without_length_exceeds_limit(tmp_path):
    class Account:
        async def resolve_attachment(self, *args):
            return {"url": "https://media.example/file", "filename": "file", "kind": "document"}
    class Content:
        def __init__(self):
            self.chunks = iter((b"x" * 20, b"y" * 20, b""))
        async def read(self, size):
            return next(self.chunks)
    class Response:
        status = 200
        content_length = None
        content = Content()
        def release(self):
            pass
    class Session:
        async def close(self):
            pass
    async def scenario():
        media, _ = manager(tmp_path, Account())
        async def request(*args, **kwargs):
            return Session(), Response()
        media._request = request
        transfer = await media.start_download("large", 1, 2, 0)
        await asyncio.gather(*list(media._tasks))
        status = await media.get_transfer_status(transfer["transfer_id"])
        assert status["status"] == "failed"
        assert status["error"]["code"] == "FILE_TOO_LARGE"
        assert list(media.config.download_dir.iterdir()) == []
    asyncio.run(scenario())


# При ошибке второго вложения первый снимок удаляется вместе с его записью.
def test_partial_preparation_cleans_only_new_snapshots(tmp_path, monkeypatch):
    media, incoming = manager(tmp_path)
    monkeypatch.setattr(media_module, "_open_source", lambda path, roots, session: os.open(path, os.O_RDONLY))
    allow_test_deletion_on_windows(media, monkeypatch)
    original = incoming / "original.txt"
    original.write_bytes(b"original")

    async def scenario():
        with pytest.raises(BridgeError):
            await media.prepare_attachments([{"path": str(original)}, {"kind": "unknown", "path": "x"}])
        assert not list(media.config.storage_dir.iterdir())
        assert original.read_bytes() == b"original"
    asyncio.run(scenario())


# Другие запросы продолжают работать, пока большой снимок копируется в рабочем потоке.
def test_snapshot_copy_does_not_block_event_loop(tmp_path, monkeypatch):
    media, incoming = manager(tmp_path)
    monkeypatch.setattr(media_module, "_open_source", lambda path, roots, session: os.open(path, os.O_RDONLY))
    entered, resume = threading.Event(), threading.Event()
    original_copy = media._copy_snapshot
    def slow_copy(*args):
        entered.set()
        if not resume.wait(5):
            raise RuntimeError("Копирование не возобновилось")
        return original_copy(*args)
    monkeypatch.setattr(media, "_copy_snapshot", slow_copy)
    original = incoming / "original.txt"
    original.write_bytes(b"original")
    async def scenario():
        task = asyncio.create_task(media.prepare_attachments([{"path": str(original)}]))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            assert not task.done()
        finally:
            resume.set()
        assert len(await task) == 1
    asyncio.run(scenario())


# Черновик защищает связанный снимок, а копия от повторной подготовки удаляется.
def test_cleanup_preserves_linked_snapshot(tmp_path, monkeypatch):
    media, incoming = manager(tmp_path)
    monkeypatch.setattr(media_module, "_open_source", lambda path, roots, session: os.open(path, os.O_RDONLY))
    allow_test_deletion_on_windows(media, monkeypatch)
    original = incoming / "original.txt"
    original.write_bytes(b"original")
    async def scenario():
        linked = (await media.prepare_attachments([{"path": str(original)}]))[0]
        extra = (await media.prepare_attachments([{"path": str(original)}]))[0]
        media.state.prepare("draft", {"attachments": [linked]})
        linked_record = media.state.get_asset(file_id=linked["file_id"])
        extra_record = media.state.get_asset(file_id=extra["file_id"])
        if os.name == "nt":
            os.chmod(extra_record["path"], 0o600)
        await media.remove_unclaimed_snapshots([linked, extra])
        assert media.state.get_asset(file_id=linked["file_id"]) == linked_record
        assert Path(linked_record["path"]).read_bytes() == b"original"
        assert media.state.get_asset(file_id=extra["file_id"]) is None
        assert not Path(extra_record["path"]).exists()
        assert original.read_bytes() == b"original"
    asyncio.run(scenario())


# Истёкшая копия освобождает квоту, а изменённая остаётся нетронутой.
def test_expired_cleanup_releases_quota_and_preserves_changed(tmp_path, monkeypatch):
    media, incoming = manager(tmp_path)
    monkeypatch.setattr(media_module, "_open_source", lambda path, roots, session: os.open(path, os.O_RDONLY))
    allow_test_deletion_on_windows(media, monkeypatch)
    media.config.max_storage_bytes = 16
    original = incoming / "original"
    original.write_bytes(b"12345678")
    async def scenario():
        first, changed = await media.prepare_attachments([{"path": str(original)}, {"path": str(original)}])
        for public in (first, changed):
            record = media.state.get_asset(file_id=public["file_id"])
            record["created_at"] = 0
            media.state.save_asset(record)
        changed_path = Path(media.state.get_asset(file_id=changed["file_id"])["path"])
        os.chmod(changed_path, 0o600)
        changed_path.write_bytes(b"changed!")
        counts = await media.cleanup_expired()
        assert counts == {"deleted": 1, "missing": 0, "skipped": 1, "retry": 0}
        assert changed_path.read_bytes() == b"changed!"
        assert media.state.get_asset(file_id=changed["file_id"]) is not None
        assert await media.cleanup_expired() == {"deleted": 0, "missing": 0, "skipped": 0, "retry": 0}
        assert len(await media.prepare_attachments([{"path": str(original)}])) == 1
    asyncio.run(scenario())


# После сбоя между удалением и подтверждением база завершает незавершённое удаление.
def test_cleanup_recovers_after_unlink(tmp_path, monkeypatch):
    media, incoming = manager(tmp_path)
    monkeypatch.setattr(media_module, "_open_source", lambda path, roots, session: os.open(path, os.O_RDONLY))
    allow_test_deletion_on_windows(media, monkeypatch)
    original = incoming / "original"
    original.write_bytes(b"12345678")
    async def scenario():
        asset = (await media.prepare_attachments([{"path": str(original)}]))[0]
        record = media.state.get_asset(file_id=asset["file_id"])
        record["created_at"] = 0
        media.state.save_asset(record)
        claimed = media.state.claim_expired_assets(1)
        assert len(claimed) == 1 and media._delete_managed(claimed[0]) == "deleted"
        reopened = StateStore(tmp_path / "state.db")
        reopened.recover_asset_deletions()
        media.state = reopened
        assert await media.cleanup_expired() == {"deleted": 0, "missing": 1, "skipped": 0, "retry": 0}
        assert reopened.get_asset(file_id=asset["file_id"]) is None
    asyncio.run(scenario())


# Каталог обмена с защитным битом и правами доверенной группы допускает очистку.
@pytest.mark.skipif(os.name != "posix", reason="Защищённое удаление реализовано для Linux")
def test_expired_download_in_group_writable_inbox(tmp_path):
    media, _ = manager(tmp_path)
    os.chmod(media.config.download_dir, 0o1770)
    file_id = uuid.uuid4().hex
    path = media.config.download_dir / file_id
    contents = b"downloaded"
    path.write_bytes(contents)
    info = path.stat()
    media.state.save_asset({
        "file_id": file_id, "path": str(path), "size": len(contents),
        "sha256": hashlib.sha256(contents).hexdigest(), "device": info.st_dev,
        "inode": info.st_ino, "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns,
        "status": "ready", "created_at": 0,
    })

    async def scenario():
        assert await media.cleanup_expired() == {"deleted": 1, "missing": 0, "skipped": 0, "retry": 0}
        assert not path.exists()
        assert media.state.get_asset(file_id=file_id) is None
    asyncio.run(scenario())


# Групповая запись без защитного бита не позволяет удалить файл.
@pytest.mark.skipif(os.name != "posix", reason="Защищённое удаление реализовано для Linux")
def test_cleanup_preserves_file_in_unprotected_group_directory(tmp_path):
    media, _ = manager(tmp_path)
    os.chmod(media.config.download_dir, 0o770)
    file_id = uuid.uuid4().hex
    path = media.config.download_dir / file_id
    contents = b"downloaded"
    path.write_bytes(contents)
    info = path.stat()
    media.state.save_asset({
        "file_id": file_id, "path": str(path), "size": len(contents),
        "sha256": hashlib.sha256(contents).hexdigest(), "device": info.st_dev,
        "inode": info.st_ino, "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns,
        "status": "ready", "created_at": 0,
    })

    async def scenario():
        assert await media.cleanup_expired() == {"deleted": 0, "missing": 0, "skipped": 1, "retry": 0}
        assert path.read_bytes() == contents
        assert media.state.get_asset(file_id=file_id) is not None
        assert await media.cleanup_expired() == {"deleted": 0, "missing": 0, "skipped": 0, "retry": 0}
    asyncio.run(scenario())


# Файл с прежним именем и другим содержимым либо ссылкой сохраняется.
@pytest.mark.skipif(os.name != "posix", reason="Защищённое удаление реализовано для Linux")
def test_cleanup_refuses_replacement_and_symlink(tmp_path):
    media, incoming = manager(tmp_path)
    original = incoming / "original"
    original.write_bytes(b"12345678")
    async def scenario():
        asset = (await media.prepare_attachments([{"path": str(original)}]))[0]
        record = media.state.get_asset(file_id=asset["file_id"])
        path = Path(record["path"])
        path.unlink()
        path.write_bytes(b"12345678")
        assert media._delete_managed(record) == "skipped"
        path.unlink()
        path.symlink_to(original)
        assert media._delete_managed(record) == "skipped"
        assert original.read_bytes() == b"12345678"
    asyncio.run(scenario())


# Локальный HTTPS-стенд проверяет поток, переходы и загрузку через открытый дескриптор.
@pytest.mark.skipif(os.name != "posix", reason="Защищённое открытие реализовано для Linux")
def test_https_transfer_and_restart(tmp_path, monkeypatch):
    certificate = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-keyout", str(key), "-out", str(certificate), "-subj", "/CN=media.example",
        "-addext", "subjectAltName=DNS:media.example",
    ], check=True, capture_output=True)
    server_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ssl.load_cert_chain(certificate, key)
    client_ssl = ssl.create_default_context(cafile=str(certificate))
    received = []

    async def serve(request):
        if request.path == "/redirect":
            raise web.HTTPFound("https://127.0.0.1/private")
        if request.path == "/redirect_dns":
            raise web.HTTPFound("https://private.example/private")
        if request.path == "/redirect_ok":
            raise web.HTTPFound("https://media.example/good")
        if request.path == "/broken":
            response = web.StreamResponse(status=200, headers={"Content-Length": "20"})
            await response.prepare(request)
            await response.write(b"partial")
            request.transport.close()
            return response
        if request.path == "/large":
            response = web.StreamResponse(status=200)
            await response.prepare(request)
            await response.write(b"x" * 40)
            await response.write_eof()
            return response
        if request.path == "/upload":
            received.append(await request.read())
            return web.Response(body=b"ok")
        return web.Response(body=b"ready")

    class Account:
        path = "/good"
        async def resolve_attachment(self, *args):
            return {"url": "https://media.example" + self.path, "filename": "one.txt", "kind": "document"}
        async def prepare_upload(self, asset):
            # Замена имени после проверки не меняет уже открытый файл.
            replacement = tmp_path / "replacement"
            replacement.write_bytes(b"changed")
            os.replace(replacement, asset["path"])
            return {"url": "https://media.example/upload", "method": "POST", "headers": {}}
        async def finish_upload(self, prepared, response, status, headers):
            assert status == 200 and response == b"ok"
            return {"uploaded": True}
        async def cancel_upload(self, prepared):
            pass

    async def scenario():
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", serve)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server_ssl)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        original_connector = media_module.aiohttp.TCPConnector
        original_resolve = media_module._PublicResolver.resolve
        loop = asyncio.get_running_loop()
        original_lookup = loop.getaddrinfo

        async def lookup(host, port, **kwargs):
            if host == "private.example":
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]
            return await original_lookup(host, port, **kwargs)

        # Только стенд подменяет DNS и доверяет своему временному сертификату.
        async def local_resolve(self, host, port=0, family=socket.AF_UNSPEC):
            if host != "media.example":
                return await original_resolve(self, host, port, family)
            return [{"hostname": host, "host": "127.0.0.1", "port": site_port,
                     "family": socket.AF_INET, "proto": 0, "flags": socket.AI_NUMERICHOST}]
        site_port = port
        monkeypatch.setattr(loop, "getaddrinfo", lookup)
        monkeypatch.setattr(media_module._PublicResolver, "resolve", local_resolve)
        monkeypatch.setattr(media_module.aiohttp, "TCPConnector", lambda **kwargs: original_connector(ssl=client_ssl, **kwargs))
        account = Account()
        media, incoming = manager(tmp_path, account)
        media.config.allowed_media_hosts = ("media.example", "private.example")
        try:
            async def download(key, path):
                account.path = path
                item = await media.start_download(key, 1, 2, 0)
                await asyncio.gather(*list(media._tasks))
                return await media.get_transfer_status(item["transfer_id"])
            good = await download("good", "/good")
            assert good["status"] == "ready"
            assert open(good["result"]["path"], "rb").read() == b"ready"
            assert (await download("redirect_ok", "/redirect_ok"))["status"] == "ready"
            for name, path, code in (("redirect", "/redirect", "FILE_HOST_NOT_APPROVED"),
                                     ("redirect_dns", "/redirect_dns", "FILE_HOST_NOT_APPROVED"),
                                     ("large", "/large", "FILE_TOO_LARGE"),
                                     ("broken", "/broken", "UPSTREAM_ERROR")):
                status = await download(name, path)
                assert status["status"] == "failed" and status["error"]["code"] == code
            assert not list(media.config.download_dir.glob("*.part"))
            original = incoming / "original.txt"
            original.write_bytes(b"original")
            asset = (await media.prepare_attachments([{"path": str(original)}]))[0]
            assert await media.upload_prepared([asset]) == [{"uploaded": True}]
            assert received == [b"original"]
            # После повторного открытия базы квитанция скачивания не исчезает.
            reopened = StateStore(tmp_path / "state.db")
            assert reopened.get_transfer(request_key="good")["result"] == good["result"]
        finally:
            await media.shutdown()
            await runner.cleanup()
    asyncio.run(scenario())
