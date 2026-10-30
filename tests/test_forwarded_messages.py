"""Пересланный текст и файлы сохраняются без подмены автора и раскрытия ссылок."""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from max_account.account import Adapter
from max_account.messages import normalize_message


# Форма взята из настоящего ответа MAX; содержание и идентификаторы заменены.
def forwarded_message():
    return {
        "id": 30, "time": 1793092685387, "sender": 10, "text": "",
        "attaches": [],
        "link": {
            "type": "FORWARD", "chatId": 900,
            "message": {
                "id": 20, "time": 1793000000000, "sender": 11,
                "text": "Текст приглашения", "attaches": [
                    {"_type": "FILE", "fileId": 75, "name": "приглашение.pdf", "size": 100, "token": "secret"},
                ],
            },
        },
    }


class ForwardedTests(unittest.IsolatedAsyncioTestCase):
    # Слова оригинала не становятся словами переславшего, файл остаётся доступным.
    def test_forward_preserves_content_and_separates_authors(self):
        result = normalize_message(forwarded_message(), "100")
        self.assertEqual(result["text"], "")
        self.assertEqual(result["sender"]["id"], "10")
        self.assertIsNone(result["reply_to"])
        original = result["forwarded"]["message"]
        self.assertEqual(original["text"], "Текст приглашения")
        self.assertEqual(original["sender"]["id"], "11")
        self.assertEqual(result["attachments"][0]["attachment_id"], "forward:75")
        self.assertNotIn("secret", json.dumps(result))

    # Вложенный оригинал может скрывать автора и время: содержание всё равно видно.
    def test_hidden_forward_source_keeps_text_without_inventing_identity(self):
        item = forwarded_message()
        original = item["link"]["message"]
        for field in ("id", "time", "sender"):
            original.pop(field)
        result = normalize_message(item, "100")["forwarded"]["message"]
        self.assertEqual(result["text"], "Текст приглашения")
        self.assertIsNone(result["time_utc"])
        self.assertIsNone(result["sender"]["id"])

    # Неограниченная цепочка пересылок не вызывает бесконечную рекурсию.
    def test_forward_depth_is_bounded(self):
        item = forwarded_message()
        current = item
        for _ in range(8):
            child = forwarded_message()
            current["link"]["message"] = child
            current = child
        result = normalize_message(item, "100")
        for _ in range(4):
            result = result["forwarded"]["message"]
        self.assertEqual(result["forwarded"]["status"], "depth_limit")

    # Скачивание обращается к порученному сообщению, а не читает чужой исходный чат.
    async def test_forwarded_file_resolves_in_destination_chat(self):
        adapter = Adapter(SimpleNamespace())
        service = SimpleNamespace(get_file_by_id=AsyncMock(return_value=SimpleNamespace(url="https://fd.oneme.ru/file?token=secret", unsafe=False)))
        with patch.object(adapter, "get_messages", return_value=[forwarded_message()]) as fetch, patch.object(adapter, "_app", return_value=SimpleNamespace(api=SimpleNamespace(messages=service))):
            result = await adapter.resolve_attachment("100", "30", "forward:75")
        fetch.assert_awaited_once_with("100", ["30"])
        service.get_file_by_id.assert_awaited_once_with(100, 30, 75)
        self.assertEqual(result["kind"], "file")
