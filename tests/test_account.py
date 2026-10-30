import unittest
from types import SimpleNamespace
from unittest.mock import patch

from max_account.account import Adapter, NoInteractiveAuthFlow
from max_account.errors import BridgeError


class AccountTests(unittest.IsolatedAsyncioTestCase):
    async def test_dialog_names_only_use_confirmed_other_participant(self):
        from unittest.mock import AsyncMock
        from pymax.types import User

        # Профиль владельца и посторонние участники не должны стать именем диалога.
        chats = [
            {"id": 10, "type": "DIALOG", "participants": {1: 0, 2: 0}},
            {"id": 11, "type": "DIALOG", "participants": {1: 0}},
            {"id": 12, "type": "DIALOG", "participants": {1: 0, 2: 0, 3: 0}},
            {"id": 13, "type": "CHAT", "participants": {1: 0, 2: 0}, "title": "Группа"},
        ]
        users = AsyncMock(return_value=[User.model_validate({"id": 2, "names": [{"firstName": "Анна", "lastName": "Иванова"}]})])
        invoke = AsyncMock(return_value=SimpleNamespace(payload={"chats": chats, "marker": 90}))
        app = SimpleNamespace(invoke=invoke, api=SimpleNamespace(users=SimpleNamespace(get_users=users)))
        adapter = Adapter(SimpleNamespace())
        adapter.account_id = "1"
        with patch.object(adapter, "_app", return_value=app):
            page = await adapter.list_chats(100)
        result = page["chats"]
        from pymax.protocol import Opcode
        invoke.assert_awaited_once_with(Opcode.CHATS_LIST, {"marker": 100})
        self.assertEqual(page["marker"], 90)
        self.assertEqual(result[0]["title"], "Анна Иванова")
        self.assertNotIn("title", result[1])
        self.assertNotIn("title", result[2])
        self.assertEqual(result[3]["title"], "Группа")
        users.assert_awaited_once_with([2])

    async def test_single_dialog_name_and_unavailable_profile(self):
        from unittest.mock import AsyncMock
        from pymax.api.chats.service import Chat
        from pymax.types import User

        # Поведение совпадает при запросе одного чата и при отказе сервиса профилей.
        chat = Chat.model_validate({"id": 10, "type": "DIALOG", "status": "ACTIVE", "owner": 1, "participants": {"1": 0, "2": 0}})
        users = AsyncMock(return_value=[User.model_validate({"id": 2, "names": [{"name": "Анна"}]})])
        app = SimpleNamespace(api=SimpleNamespace(chats=SimpleNamespace(get_chat=AsyncMock(return_value=chat)), users=SimpleNamespace(get_users=users)))
        adapter = Adapter(SimpleNamespace())
        adapter.account_id = "1"
        with patch.object(adapter, "_app", return_value=app):
            self.assertEqual((await adapter.get_chat("10"))["title"], "Анна")
            users.side_effect = RuntimeError("профиль недоступен")
            with self.assertRaises(BridgeError) as raised:
                await adapter.get_chat("10")
            self.assertEqual(raised.exception.code, "UPSTREAM_ERROR")
            self.assertTrue(raised.exception.retryable)

    async def test_noninteractive_auth_never_requests_credentials(self):
        with self.assertRaises(BridgeError) as raised:
            await NoInteractiveAuthFlow().authenticate(None)
        self.assertEqual(raised.exception.code, "AUTH_REQUIRED")

    async def test_submit_preserves_text_and_cid_without_retry(self):
        try:
            from pymax.protocol import Opcode
        except ImportError:
            self.skipTest("PyMax не установлен")
        calls = []

        class App:
            async def invoke(self, opcode, payload):
                calls.append((opcode, payload))
                return SimpleNamespace(payload={"id": 44})

        adapter = Adapter(SimpleNamespace())
        with patch.object(adapter, "_app", return_value=App()):
            result = await adapter.submit({"chat_id": "10", "text": "**точно**", "reply_to": "5"}, 12345, [{"_type": "FILE", "fileId": 6}])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], Opcode.MSG_SEND)
        self.assertEqual(calls[0][1]["message"], {"text": "**точно**", "cid": 12345, "elements": [], "attaches": [{"_type": "FILE", "fileId": 6}], "link": {"type": "REPLY", "messageId": 5}})
        self.assertEqual(result["message_id"], "44")

    async def test_transport_disables_proxy_and_never_logs_frame(self):
        from pathlib import Path
        from unittest.mock import AsyncMock
        from max_account.account import make_client

        settings = SimpleNamespace(session_dir=Path("."), request_timeout=30, upload_timeout=900)
        client = make_client(settings)
        transport = client._build_connection().transport
        socket = SimpleNamespace(close_code=None, send=AsyncMock())
        with patch("websockets.asyncio.client.connect", new=AsyncMock(return_value=socket)) as connect:
            await transport.connect()
            await transport.send("секрет")
        self.assertIsNone(connect.await_args.kwargs["proxy"])
        socket.send.assert_awaited_once_with("секрет")
        self.assertFalse(client.extra_config.telemetry)
        self.assertFalse(client.extra_config.relogin)

    async def test_document_waiter_is_installed_before_upload_and_removed(self):
        from pymax.protocol import Opcode
        waiters = {}

        class App:
            api = SimpleNamespace(uploads=SimpleNamespace(file_upload_waiters=waiters))

            async def invoke(self, opcode, payload):
                self.opcode = opcode
                return SimpleNamespace(payload={"info": [{"url": "https://example.invalid/upload", "fileId": 87}]})

        app = App()
        adapter = Adapter(SimpleNamespace())
        with patch.object(adapter, "_app", return_value=app):
            prepared = await adapter.prepare_upload({"kind": "document", "filename": "file.txt", "size": 5})
            self.assertEqual(app.opcode, Opcode.FILE_UPLOAD)
            self.assertIn(87, waiters)
            waiters[87].set_result(object())
            result = await adapter.finish_upload(prepared, b"", 200, {})
        self.assertEqual(result, {"_type": "FILE", "fileId": 87})
        self.assertEqual(waiters, {})

    async def test_account_mismatch_closes_session_before_access(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import AsyncMock

        with tempfile.TemporaryDirectory() as directory:
            config = SimpleNamespace(session_dir=Path(directory), expected_account_id="100")
            app = SimpleNamespace(start=AsyncMock(), store=SimpleNamespace(load_session=AsyncMock(return_value=object())), me=SimpleNamespace(contact=SimpleNamespace(id=200)))
            client = SimpleNamespace(_ensure_runtime=AsyncMock(), _app=app, close=AsyncMock())
            adapter = Adapter(config)
            with patch("max_account.account.make_client", return_value=client):
                with self.assertRaises(BridgeError) as raised:
                    await adapter.start()
            self.assertEqual(raised.exception.code, "ACCOUNT_MISMATCH")
            client.close.assert_awaited_once()
            self.assertFalse(adapter.status()["connected"])

    async def test_missing_session_does_not_open_network(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import AsyncMock

        with tempfile.TemporaryDirectory() as directory:
            settings = SimpleNamespace(session_dir=Path(directory), request_timeout=2, upload_timeout=10, expected_account_id="1")
            adapter = Adapter(settings)
            with patch("websockets.asyncio.client.connect", new=AsyncMock()) as connect:
                with self.assertRaises(BridgeError) as raised:
                    await adapter.start()
                self.assertEqual(raised.exception.code, "AUTH_REQUIRED")
                self.assertEqual(adapter.status()["reason"], "AUTH_REQUIRED")
                connect.assert_not_awaited()
                await adapter.close()

    async def test_network_loss_is_reported_and_recovers_without_sending(self):
        import asyncio
        import tempfile
        from pathlib import Path
        from unittest.mock import AsyncMock

        class Connection:
            def __init__(self):
                self.is_open = True
                self.closed = asyncio.Event()

            async def wait_closed(self):
                await self.closed.wait()

        class Client:
            def __init__(self):
                connection = Connection()
                self._app = SimpleNamespace(
                    connection=connection,
                    store=SimpleNamespace(load_session=AsyncMock(return_value=object())),
                    me=SimpleNamespace(contact=SimpleNamespace(id=1)),
                    start=AsyncMock(),
                )
                self._ensure_runtime = AsyncMock()
                self.close = AsyncMock()

        with tempfile.TemporaryDirectory() as directory:
            settings = SimpleNamespace(session_dir=Path(directory), expected_account_id="1")
            clients = [Client(), Client()]
            adapter = Adapter(settings)
            real_sleep = asyncio.sleep

            async def immediate_sleep(delay):
                await real_sleep(0)

            with patch("max_account.account.make_client", side_effect=clients), patch("max_account.account.asyncio.sleep", side_effect=immediate_sleep):
                await adapter.start()
                first = clients[0]._app.connection
                first.is_open = False
                self.assertFalse(adapter.status()["connected"])
                first.closed.set()
                for _ in range(100):
                    if adapter.client is clients[1] and adapter.status()["connected"]:
                        break
                    await real_sleep(0)
                self.assertTrue(adapter.status()["connected"])
                self.assertEqual(adapter.status()["account_id"], "1")
                await adapter.close()

    async def test_audio_resolves_only_reported_url(self):
        adapter = Adapter(SimpleNamespace())
        audio = {"_type": "AUDIO", "audioId": 75, "url": "https://media.invalid/audio?token=secret", "token": "secret", "duration": 3000}
        with patch.object(adapter, "get_messages", return_value=[{"id": 20, "attaches": [audio]}]), patch.object(adapter, "_app", return_value=SimpleNamespace(api=SimpleNamespace(messages=object()))):
            result = await adapter.resolve_attachment("10", "20", "75")
        self.assertEqual(result["url"], audio["url"])
        self.assertEqual(result["kind"], "audio")
        self.assertEqual(result["filename"], "audio")

    async def test_audio_without_id_uses_index_and_missing_url_fails(self):
        adapter = Adapter(SimpleNamespace())
        audio = {"_type": "AUDIO", "audioId": None, "url": None, "token": "secret"}
        with patch.object(adapter, "get_messages", return_value=[{"id": 20, "attaches": [audio]}]), patch.object(adapter, "_app", return_value=SimpleNamespace(api=SimpleNamespace(messages=object()))):
            with self.assertRaises(BridgeError) as raised:
                await adapter.resolve_attachment("10", "20", "0")
        self.assertEqual(raised.exception.code, "MEDIA_UNSUPPORTED")

    async def test_sdk_child_logs_do_not_reach_root_after_client_setup(self):
        import io
        import logging
        import tempfile
        from pathlib import Path
        from unittest.mock import AsyncMock
        from max_account.account import make_client, silence_network_logs
        from pymax.logging import get_logger

        root = logging.getLogger()
        names = ("pymax", "websockets", "aiohttp", "httpcore", "httpx", "httpx2", "mcp")
        saved = [(logging.getLogger(name), list(logging.getLogger(name).handlers), logging.getLogger(name).propagate, logging.getLogger(name).disabled, logging.getLogger(name).level) for name in names]
        prior_handlers, prior_level = list(root.handlers), root.level
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        root.handlers = [handler]
        root.setLevel(logging.WARNING)
        try:
            with tempfile.TemporaryDirectory() as directory:
                config = SimpleNamespace(session_dir=Path(directory), request_timeout=2, upload_timeout=10)
                for interactive in (False, True):
                    silence_network_logs()
                    client = make_client(config, interactive=interactive)
                    with patch("websockets.asyncio.client.connect", new=AsyncMock()) as connect:
                        await client._ensure_runtime()
                        connect.assert_not_awaited()
                    get_logger("auth.qr").warning("секрет-маркер")
                    logging.getLogger("websockets.client").warning("секрет-маркер")
                    logging.getLogger("aiohttp.client").warning("секрет-маркер")
                    await client.close()
            logging.getLogger("проверка").warning("обычный-журнал")
            self.assertIn("обычный-журнал", stream.getvalue())
            self.assertNotIn("секрет-маркер", stream.getvalue())
        finally:
            root.handlers, root.level = prior_handlers, prior_level
            for logger, handlers, propagate, disabled, level in saved:
                logger.handlers = handlers
                logger.propagate, logger.disabled, logger.level = propagate, disabled, level

    async def test_connect_timeout_starts_watcher_and_retries(self):
        import asyncio
        from unittest.mock import AsyncMock

        settings = SimpleNamespace(request_timeout=0.02)
        adapter = Adapter(settings)
        attempted = asyncio.Event()
        hold = asyncio.Event()
        connection = SimpleNamespace(is_open=True, wait_closed=AsyncMock(side_effect=hold.wait))
        client = SimpleNamespace(_app=SimpleNamespace(connection=connection), close=AsyncMock())
        count = 0
        real_sleep = asyncio.sleep

        async def connect():
            nonlocal count
            count += 1
            if count == 1:
                await hold.wait()
            adapter.client = client
            adapter.account_id = "1"
            adapter._ready = True
            adapter._last_error = None
            attempted.set()

        async def fast_retry(delay):
            await real_sleep(0)

        with patch.object(adapter, "_connect_once", side_effect=connect), patch("max_account.account.asyncio.sleep", side_effect=fast_retry):
            with self.assertRaises(BridgeError) as raised:
                await adapter.start()
            self.assertEqual(raised.exception.code, "UPSTREAM_ERROR")
            self.assertTrue(raised.exception.retryable)
            await asyncio.wait_for(attempted.wait(), 1)
            self.assertTrue(adapter.status()["connected"])
            self.assertGreaterEqual(count, 2)
            await adapter.close()
        client.close.assert_awaited_once()

    async def test_external_cancellation_does_not_start_watcher(self):
        import asyncio

        adapter = Adapter(SimpleNamespace(request_timeout=10))
        entered = asyncio.Event()
        block = asyncio.Event()

        async def connect():
            entered.set()
            await block.wait()

        with patch.object(adapter, "_connect_once", side_effect=connect):
            task = asyncio.create_task(adapter.start())
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertIsNone(adapter._monitor)
            await adapter.close()

    async def test_slow_sdk_start_is_closed_when_initial_attempt_times_out(self):
        import asyncio
        import tempfile
        from pathlib import Path
        from unittest.mock import AsyncMock

        entered = asyncio.Event()
        blocked = asyncio.Event()

        async def slow_start():
            entered.set()
            await blocked.wait()

        with tempfile.TemporaryDirectory() as directory:
            settings = SimpleNamespace(session_dir=Path(directory), expected_account_id="1", request_timeout=0.02)
            app = SimpleNamespace(start=slow_start, store=SimpleNamespace(load_session=AsyncMock(return_value=object())))
            client = SimpleNamespace(_ensure_runtime=AsyncMock(), _app=app, close=AsyncMock())
            adapter = Adapter(settings)
            with patch("max_account.account.make_client", return_value=client):
                with self.assertRaises(BridgeError) as raised:
                    await adapter.start()
                self.assertTrue(entered.is_set())
                self.assertEqual(raised.exception.code, "UPSTREAM_ERROR")
                self.assertIsNotNone(adapter._monitor)
                client.close.assert_awaited_once()
                await adapter.close()
