"""Сквозные проверки HTTP, двух клиентов и восстановления без личного аккаунта."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import json
import hashlib
import os
from pathlib import Path
import socket
import tempfile
import unittest
import uuid

import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
import uvicorn

from max_account.app import create_app, InstanceLock
from max_account.config import Settings, prepare_directories
from max_account.errors import BridgeError
from max_account.state import StateStore


# Управляемый аккаунт считает реальные обращения адаптера и сохраняет точный отправленный текст.
class ControlledAccount:
    account_id = "1"

    def __init__(self, config):
        self.connected = False
        self.sent = []
        self.history_calls = 0

    async def start(self):
        self.connected = True

    async def close(self):
        self.connected = False

    def status(self):
        return {"connected": self.connected, "account_id": self.account_id}

    async def get_chat(self, chat_id):
        return {"id": int(chat_id), "title": "Проверка", "type": "DIALOG"}

    async def get_history(self, chat_id, **kwargs):
        self.history_calls += 1
        if self.history_calls == 1:
            return [{"id": 91, "time": 1793210400000, "text": "Нужная страница", "sender": 1}]
        return []

    async def submit(self, payload, cid, attachments):
        self.sent.append((payload, cid, attachments))
        await asyncio.sleep(0.03)
        return {"chat_id": payload["chat_id"], "message_id": "777", "cid": str(cid)}


# Настройки изолируют файлы проверки от установленной службы и личных данных.
def settings_at(root):
    settings = Settings(
        session_dir=root / "private/session", storage_dir=root / "private/spool",
        download_dir=root / "inbox", input_roots=(root / "outbox",),
        token_file=root / "private/mcp.key", state_db=root / "private/state.db",
    )
    prepare_directories(settings)
    settings.token_file.write_text("a" * 48, encoding="ascii")
    settings.token_file.chmod(0o600)
    return settings


# Используем настоящий TCP и срок жизни приложения, включая освобождение межпроцессной блокировки.
@asynccontextmanager
async def running(settings, factory=ControlledAccount):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    settings = replace(settings, port=sock.getsockname()[1])
    app = create_app(settings, account_factory=factory)
    server = uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="critical", lifespan="on"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if task.done():
                    await task
                    raise RuntimeError("Служба не запустилась")
                await asyncio.sleep(0.01)
        yield app, f"http://127.0.0.1:{settings.port}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)
        sock.close()


# Оба порядка протокола МСП работают через один защищённый HTTP-вход.
@asynccontextmanager
async def connected(url, mode="auto"):
    async with httpx2.AsyncClient(headers={"Authorization": "Bearer " + "a" * 48}, trust_env=False,
                                 timeout=httpx2.Timeout(10, read=30)) as http:
        async with Client(streamable_http_client(url + "/mcp", http_client=http), mode=mode) as client:
            yield client


def value(result):
    if result.structured_content is not None:
        return result.structured_content
    return json.loads(result.content[0].text)


class AppTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipIf(os.name == "nt", "Физическая очистка проверяется на целевом Linux")
    async def test_startup_cleanup_preserves_sent_receipt_and_source(self):
        with tempfile.TemporaryDirectory() as folder:
            settings = replace(settings_at(Path(folder)), retention_seconds=1)
            original = settings.input_roots[0] / "original.txt"
            original.write_bytes(b"source-kept")
            file_id = uuid.uuid4().hex
            snapshot = settings.storage_dir / file_id
            snapshot.write_bytes(original.read_bytes())
            snapshot.chmod(0o400)
            info = snapshot.stat()
            asset = {"file_id": file_id, "path": str(snapshot), "kind": "document",
                     "filename": original.name, "size": info.st_size,
                     "sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(), "created_at": 0,
                     "device": info.st_dev, "inode": info.st_ino, "status": "ready"}
            asset.update({"mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns})
            store = StateStore(settings.state_db)
            store.save_asset(asset)
            attachment = {key: asset[key] for key in ("file_id", "kind", "filename", "size", "sha256")}
            operation = store.prepare("sent-before-cleanup", {"chat_id": "10", "text": "текст",
                                                            "reply_to": None, "attachments": [attachment]})
            store.claim(operation["operation_id"])
            store.mark_sending(operation["operation_id"])
            store.finish(operation["operation_id"], "sent", receipt={"message_id": "101"})
            async with running(settings) as (app, url):
                self.assertFalse(snapshot.exists())
                self.assertEqual(original.read_bytes(), b"source-kept")
                self.assertIsNone(app.runtime.store.get_asset(file_id=file_id))
                async with connected(url) as client:
                    status = value(await client.call_tool("get_send_status", {"request_key": "sent-before-cleanup"}))
                    self.assertEqual(status["status"], "sent")
                    self.assertEqual(status["receipt"]["message_id"], "101")

    async def test_two_http_clients_one_send_and_reopen(self):
        with tempfile.TemporaryDirectory() as folder:
            settings = settings_at(Path(folder))
            async with running(settings) as (app, url):
                async with connected(url) as first, connected(url, "legacy") as second:
                    tools = await first.list_tools()
                    self.assertEqual(len(tools.tools), 18)
                    payload = {"request_key": "одно-поручение", "chat_id": "10", "text": "**точно** _как_ передано"}
                    drafts = [value(item) for item in await asyncio.gather(
                        first.call_tool("prepare_message", payload), second.call_tool("prepare_message", payload),
                    )]
                    self.assertTrue(drafts[0]["ok"], drafts[0])
                    self.assertEqual(drafts[0]["draft_id"], drafts[1]["draft_id"])
                    draft_id = drafts[0]["draft_id"]
                    params = {"draft_id": draft_id, "expected_chat_id": "10", "user_instruction": "Проверочное поручение"}
                    await asyncio.gather(first.call_tool("send_prepared_message", params), second.call_tool("send_prepared_message", params))
                    status = value(await second.call_tool("get_send_status", {"draft_id": draft_id}))
                    self.assertEqual(status["status"], "sent", status)
                    self.assertEqual(len(app.runtime.account.sent), 1)
                    self.assertEqual(app.runtime.account.sent[0][0]["text"], payload["text"])
            async with running(settings) as (reopened, url):
                async with connected(url, "legacy") as client:
                    result = value(await client.call_tool("get_send_status", {"request_key": "одно-поручение"}))
                    self.assertEqual(result["status"], "sent")
                    self.assertEqual(result["receipt"]["message_id"], "777")
                    self.assertEqual(reopened.runtime.account.sent, [])

    async def test_batch_replay_prevents_page_loss_and_forgery(self):
        with tempfile.TemporaryDirectory() as folder:
            async with running(settings_at(Path(folder))) as (app, url):
                async with connected(url) as client:
                    batch = value(await client.call_tool("begin_batch", {"task_key": "задание", "chat_ids": ["10"], "initial_since": 1793206800000}))
                    params = {"chat_id": "10", "batch_id": batch["batch_id"]}
                    first = value(await client.call_tool("get_messages", params))
                    self.assertTrue(first["ok"], first)
                    # Первый ответ считаем потерянным: клиент повторяет прежний запрос.
                    replay = value(await client.call_tool("get_messages", params))
                    self.assertEqual(first, replay)
                    self.assertEqual(app.runtime.account.history_calls, 1)
                    forged = value(await client.call_tool("get_messages", {**params, "cursor": "чужой-курсор"}))
                    self.assertFalse(forged["ok"])
                    denied = value(await client.call_tool("commit_batch", {"batch_id": batch["batch_id"], "expected_checkpoint_version": 0}))
                    self.assertFalse(denied["ok"])
                    end = value(await client.call_tool("get_messages", {**params, "cursor": first["next_cursor"]}))
                    self.assertTrue(end["end_reached"], end)
                    committed = value(await client.call_tool("commit_batch", {"batch_id": batch["batch_id"], "expected_checkpoint_version": 0}))
                    self.assertEqual(committed["status"], "committed", committed)
                    self.assertEqual(value(await client.call_tool("get_messages", params)), first)

    async def test_auth_host_origin_and_single_owner(self):
        with tempfile.TemporaryDirectory() as folder:
            settings = settings_at(Path(folder))
            async with running(settings) as (app, url):
                async with httpx2.AsyncClient(trust_env=False) as http:
                    self.assertEqual((await http.get(url + "/health")).status_code, 401)
                    headers = {"Authorization": "Bearer " + "a" * 48}
                    self.assertEqual((await http.get(url + "/health", headers=headers)).status_code, 200)
                    # Проверка транспорта происходит до вызова любого инструмента.
                    headers.update({"Host": "evil.example", "Content-Type": "application/json"})
                    self.assertEqual((await http.post(url + "/mcp", headers=headers, content=b"{}")).status_code, 421)
                    headers.pop("Host")
                    headers["Origin"] = "https://evil.example"
                    self.assertEqual((await http.post(url + "/mcp", headers=headers, content=b"{}")).status_code, 403)
                lock = InstanceLock(settings.session_dir / "instance.lock")
                with self.assertRaises(BridgeError):
                    lock.acquire()

    async def test_service_without_account_keeps_status_available(self):
        from max_account.account import Adapter
        with tempfile.TemporaryDirectory() as folder:
            async with running(settings_at(Path(folder)), Adapter) as (app, url):
                async with connected(url, "legacy") as client:
                    status = value(await client.call_tool("account_status"))
                    self.assertFalse(status["connected"])
                    self.assertEqual(status["state"], "AUTH_REQUIRED")
                    result = value(await client.call_tool("get_chats"))
                    self.assertEqual(result["error"]["code"], "AUTH_REQUIRED")
