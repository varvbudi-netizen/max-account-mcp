from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from max_account.account import Adapter
from max_account.errors import BridgeError
from max_account.messages import Messages


# Подставляем ответ протокола, не подключаясь к аккаунту MAX.
def adapter_with_rows(rows):
    adapter = Adapter(SimpleNamespace())
    client = SimpleNamespace(invoke=AsyncMock(return_value=SimpleNamespace(payload={"messages": rows})))
    adapter._app = lambda: client
    return adapter, client


class MessageIdentityTests(IsolatedAsyncioTestCase):
    async def test_partial_response_and_absent_chat_are_allowed(self):
        adapter, client = adapter_with_rows([{"id": 30}, {"id": "31", "chatId": "100"}])
        rows = await adapter.get_messages("100", ["30", "31", "32"])
        self.assertEqual(rows, [{"id": 30}, {"id": "31", "chatId": "100"}])
        self.assertEqual(client.invoke.await_args.args[1], {"chatId": 100, "messageIds": [30, 31, 32]})

    async def test_wrong_or_duplicate_outer_identity_is_rejected(self):
        cases = [
            [{"id": 99}],
            [{"id": 30, "chatId": 999}],
            [{"id": 30, "chat_id": 999}],
            [{"id": 30, "chatId": None}],
            [{"id": 30}, {"id": "30"}],
            [{"id": 30, "chatId": 100, "chat_id": 999}],
            [{"time": 1000}],
        ]
        for rows in cases:
            with self.subTest(rows=rows):
                adapter, _ = adapter_with_rows(rows)
                with self.assertRaises(BridgeError) as error:
                    await adapter.get_messages("100", ["30"])
                self.assertEqual(error.exception.code, "UPSTREAM_ERROR")
                self.assertNotIn("999", str(error.exception))

    async def test_context_rejects_wrong_message_before_history(self):
        adapter, _ = adapter_with_rows([{"id": 99, "time": 1000, "text": "чужое"}])
        adapter.get_history = AsyncMock()
        with self.assertRaises(BridgeError):
            await Messages(adapter).get_context("100", "30")
        adapter.get_history.assert_not_awaited()

    async def test_context_keeps_forward_source_and_separate_author(self):
        row = {
            "id": 30, "time": 1000, "sender": 10, "text": "",
            "link": {"type": "FORWARD", "chatId": 999, "message": {
                "id": 99, "chatId": 999, "time": 900, "sender": 20, "text": "текст",
            }},
        }
        adapter, _ = adapter_with_rows([row])
        adapter.get_history = AsyncMock(return_value=[])
        context = await Messages(adapter).get_context("100", "30")
        self.assertEqual(context["message"]["sender"]["id"], "10")
        self.assertEqual(context["message"]["forwarded"]["message"]["sender"]["id"], "20")
        adapter.get_history.assert_awaited_once_with("100", from_ms=1000, backward=10, forward=10)

    async def test_attachment_checks_outer_identity_even_if_fetch_is_replaced(self):
        for row in ({"id": 99}, {"id": 30, "chatId": 999}):
            row["link"] = {"type": "FORWARD", "message": {
                "attaches": [{"_type": "PHOTO", "photoId": 42, "baseUrl": "https://i.oneme.ru/synthetic"}],
            }}
            adapter = Adapter(SimpleNamespace())
            adapter.get_messages = AsyncMock(return_value=[row])
            with self.assertRaises(BridgeError) as error:
                await adapter.resolve_attachment("100", "30", "forward:42")
            self.assertEqual(error.exception.code, "UPSTREAM_ERROR")
            self.assertNotIn("https://", str(error.exception))

    async def test_direct_and_forwarded_files_use_outer_message(self):
        file_data = {"_type": "FILE", "fileId": 75, "name": "файл.pdf"}
        rows_and_paths = [
            ({"id": 30, "attaches": [file_data]}, "75"),
            ({"id": 30, "chatId": 100, "link": {"type": "FORWARD", "chatId": 999,
             "message": {"id": 99, "chatId": 999, "attaches": [file_data]}}}, "forward:75"),
        ]
        for row, path in rows_and_paths:
            with self.subTest(path=path):
                adapter, client = adapter_with_rows([row])
                get_file = AsyncMock(return_value=SimpleNamespace(url="https://fd.oneme.ru/synthetic", unsafe=False))
                client.api = SimpleNamespace(messages=SimpleNamespace(get_file_by_id=get_file))
                result = await adapter.resolve_attachment("100", "30", path)
                self.assertEqual(result["kind"], "file")
                get_file.assert_awaited_once_with(100, 30, 75)
