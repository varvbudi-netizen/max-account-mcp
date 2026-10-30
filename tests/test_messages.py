import unittest
from datetime import datetime, timezone

from max_account.errors import BridgeError
from max_account.messages import Messages, normalize_message


def message(ident, timestamp, text="текст", attachments=None):
    return {"id": ident, "chatId": 10, "time": timestamp, "sender": 20, "text": text, "attaches": attachments or []}


class FakeAccount:
    account_id = "1"

    def __init__(self, pages):
        self.pages = iter(pages)
        self.calls = []

    async def get_history(self, chat_id, **params):
        self.calls.append((chat_id, params))
        result = next(self.pages)
        if isinstance(result, Exception):
            raise result
        return result


class MessageTests(unittest.IsolatedAsyncioTestCase):
    async def test_chat_pages_preserve_remainder_and_server_marker(self):
        from unittest.mock import AsyncMock, patch

        # Лимит сохраняет остаток страницы и исключает повтор на границе.
        pages = {
            1000: {"chats": [{"id": 1, "type": "DIALOG", "title": "А", "lastEventTime": 950}, {"id": 2, "type": "CHAT", "title": "Б", "lastEventTime": 900}], "marker": 900},
            901: {"chats": [{"id": 2, "type": "CHAT", "title": "Б", "lastEventTime": 900}, {"id": 3, "type": "CHAT", "title": "В", "lastEventTime": 800}], "marker": 800},
            801: {"chats": [{"id": 3, "type": "CHAT", "title": "В", "lastEventTime": 800}], "marker": 0},
        }
        account = FakeAccount([])
        account.list_chats = AsyncMock(side_effect=lambda marker: pages[marker])
        service = Messages(account)
        with patch("max_account.messages.time.time", return_value=1):
            first = await service.list_chats(limit=1)
            second = await service.list_chats(limit=1, cursor=first["next_cursor"])
            third = await service.list_chats(limit=1, cursor=second["next_cursor"])
            last = await service.list_chats(cursor=third["next_cursor"])
        self.assertEqual([first["chats"][0]["chat_id"], second["chats"][0]["chat_id"], third["chats"][0]["chat_id"]], ["1", "2", "3"])
        self.assertEqual([call.args[0] for call in account.list_chats.await_args_list], [1000, 1000, 901, 801])
        self.assertTrue(last["end_reached"])
        self.assertFalse(last["partial"])

    async def test_chat_overlap_keeps_boundary_tie_and_detects_stall(self):
        from unittest.mock import AsyncMock, patch

        # Вторая запись с тем же временем попадает только на следующую страницу.
        rows = [{"id": index, "type": "CHAT", "title": str(index), "lastEventTime": 900 if index == 49 else 950} for index in range(50)]
        tied = {"id": 50, "type": "CHAT", "title": "50", "lastEventTime": 900}
        account = FakeAccount([])
        account.list_chats = AsyncMock(side_effect=[
            {"chats": rows, "marker": 900},
            {"chats": [rows[-1], tied], "marker": 0},
        ])
        service = Messages(account)
        with patch("max_account.messages.time.time", return_value=1):
            first = await service.list_chats()
            last = await service.list_chats(cursor=first["next_cursor"])
        self.assertEqual([row["chat_id"] for row in last["chats"]], ["50"])
        self.assertTrue(last["end_reached"])
        self.assertEqual(account.list_chats.await_args_list[1].args[0], 901)

        # Если все 50 чатов имеют одну отметку, сервер повторяет страницу.
        same_time = [{"id": index, "type": "CHAT", "title": str(index), "lastEventTime": 900} for index in range(50)]
        account.list_chats = AsyncMock(side_effect=[
            {"chats": same_time, "marker": 900},
            {"chats": same_time, "marker": 900},
        ])
        with patch("max_account.messages.time.time", return_value=1):
            first = await service.list_chats()
            last = await service.list_chats(cursor=first["next_cursor"])
        self.assertEqual(last["chats"], [])
        self.assertFalse(last["end_reached"])
        self.assertEqual(last["reason"], "PAGINATION_STALLED")

    async def test_chat_cursor_rejects_changed_page_and_other_account(self):
        from unittest.mock import AsyncMock, patch

        # Изменение имени не должно незаметно сдвигать остаток результатов поиска.
        account = FakeAccount([])
        account.list_chats = AsyncMock(side_effect=[
            {"chats": [{"id": 1, "type": "DIALOG", "title": "Анна"}, {"id": 2, "type": "DIALOG", "title": "Антон"}], "marker": 900},
            {"chats": [{"id": 1, "type": "DIALOG", "title": None}, {"id": 2, "type": "DIALOG", "title": "Антон"}], "marker": 900},
        ])
        service = Messages(account)
        with patch("max_account.messages.time.time", return_value=1):
            first = await service.list_chats(query="ан", limit=1)
            with self.assertRaises(BridgeError) as changed:
                await service.list_chats(query="ан", limit=1, cursor=first["next_cursor"])
            self.assertEqual(changed.exception.code, "CURSOR_INVALID")
            account.account_id = "2"
            with self.assertRaises(BridgeError) as other:
                await service.list_chats(query="ан", limit=1, cursor=first["next_cursor"])
            self.assertEqual(other.exception.code, "CURSOR_INVALID")
        self.assertEqual(account.list_chats.await_count, 2)

    async def test_chat_search_continues_through_unmatched_page_and_detects_stall(self):
        from unittest.mock import AsyncMock, patch

        account = FakeAccount([])
        account.list_chats = AsyncMock(side_effect=[
            {"chats": [{"id": 1, "type": "CHAT", "title": "Иное", "lastEventTime": 900}], "marker": 900},
            {"chats": [{"id": 2, "type": "CHAT", "title": "Нужное", "lastEventTime": 900}], "marker": 900},
        ])
        service = Messages(account)
        with patch("max_account.messages.time.time", return_value=1):
            first = await service.list_chats(query="нуж")
            with self.assertRaises(BridgeError):
                await service.list_chats(query="другое", cursor=first["next_cursor"])
            second = await service.list_chats(query="нуж", cursor=first["next_cursor"])
        self.assertEqual(first["chats"], [])
        self.assertEqual(second["chats"][0]["title"], "Нужное")
        self.assertEqual(second["reason"], "PAGINATION_STALLED")
        self.assertFalse(second["end_reached"])

    def test_unknown_attachment_and_secrets(self):
        row = normalize_message(message(1, 1000, attachments=[{"_type": "FILE", "fileId": 7, "name": "../a.txt", "token": "SECRET", "url": "https://secret"}, {"_type": "FUTURE", "token": "SECRET"}]), "10")
        self.assertEqual(row["attachments"][0]["filename"], "a.txt")
        self.assertEqual(row["attachments"][1]["kind"], "unknown")
        self.assertNotIn("SECRET", repr(row))
        self.assertEqual(row["time_utc"], "1970-01-01T00:00:01Z")

    async def test_saturated_timestamp_continues_with_larger_window(self):
        account = FakeAccount([[message(1, 1000), message(2, 1000)], [message(1, 1000), message(2, 1000), message(3, 900)]])
        reader = Messages(account)
        first = await reader.get_messages("10", limit=2, until=2000)
        second = await reader.get_messages("10", limit=2, cursor=first["next_cursor"])
        self.assertEqual([row["message_id"] for row in second["messages"]], ["3"])
        self.assertEqual(account.calls[-1][1]["backward"], 4)

    async def test_history_error_is_not_empty_history(self):
        account = FakeAccount([RuntimeError("ошибка сети")])
        with self.assertRaises(RuntimeError):
            await Messages(account).get_messages("10", until=2000)

    async def test_cursor_fixes_bounds_and_requires_empty_response(self):
        account = FakeAccount([[message(1, 1000)], []])
        service = Messages(account)
        first = await service.get_messages("10", limit=5, until=2000)
        self.assertFalse(first["end_reached"])
        with self.assertRaises(BridgeError):
            await service.get_messages("10", cursor=first["next_cursor"], until=3000)
        second = await service.get_messages("10", cursor=first["next_cursor"])
        self.assertTrue(second["end_reached"])
        self.assertEqual(account.calls[-1][1]["from_ms"], 1000)

    def test_audio_metadata_does_not_leak_url_or_token(self):
        audio = {"_type": "AUDIO", "audioId": 987, "url": "https://media.invalid/x?secret", "token": "secret", "duration": 3000}
        row = normalize_message(message(1, 1000, attachments=[audio]), "10")
        self.assertEqual(row["attachments"][0]["attachment_id"], "987")
        self.assertEqual(row["attachments"][0]["kind"], "audio")
        self.assertTrue(row["attachments"][0]["downloadable"])
        self.assertNotIn("secret", repr(row))
        without_url = normalize_message(message(2, 1000, attachments=[{"_type": "AUDIO", "audioId": None}]), "10")
        self.assertEqual(without_url["attachments"][0]["attachment_id"], "0")
        self.assertFalse(without_url["attachments"][0]["downloadable"])

    async def test_context_returns_embedded_reply_but_never_opens_other_chat(self):
        source = message(7, 1000, "исходный текст")
        target = message(8, 2000, "ответ")
        target["link"] = {"type": "REPLY", "chatId": 10, "message": source}

        class ContextAccount:
            def __init__(self, row):
                self.row = row
                self.calls = []

            async def get_messages(self, chat_id, ids):
                self.calls.append((chat_id, ids))
                return [self.row]

            async def get_history(self, chat_id, **kwargs):
                return []

        account = ContextAccount(target)
        result = await Messages(account).get_context("10", "8")
        self.assertEqual(result["reply_source"]["message"]["text"], "исходный текст")
        self.assertEqual(result["message"]["reply_to"], "7")
        target["link"]["chatId"] = 11
        result = await Messages(account).get_context("10", "8")
        self.assertEqual(result["reply_source"]["reason"], "CROSS_CHAT")
        self.assertEqual(len(account.calls), 2)

    def test_forward_is_not_reply(self):
        row = message(8, 2000)
        row["link"] = {"type": "FORWARD", "chatId": 10, "message": message(7, 1000)}
        self.assertIsNone(normalize_message(row, "10")["reply_to"])

    def test_sdk_audio_wire_format_is_supported(self):
        from pymax.types.domain.attachments.audio import AudioAttachment

        sdk_audio = AudioAttachment.model_validate({"_type": "AUDIO", "audioId": 18, "url": "https://media.invalid/a", "duration": 1000})
        wire = sdk_audio.model_dump(mode="json", by_alias=True, exclude_none=True)
        row = normalize_message(message(1, 1000, attachments=[wire]), "10")
        self.assertEqual(row["attachments"][0]["attachment_id"], "18")
        self.assertEqual(row["attachments"][0]["kind"], "audio")
        self.assertTrue(row["attachments"][0]["downloadable"])
        self.assertNotIn("media.invalid", repr(row))
