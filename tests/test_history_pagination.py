import unittest
from unittest.mock import patch

from max_account.messages import Messages


# Поддельный сервер повторяет все записи на включённой временной границе.
class HistoryAccount:
    account_id = "1"

    def __init__(self, rows):
        self.rows = sorted(rows, key=lambda row: (row["time"], row["id"]), reverse=True)
        self.calls = []

    async def get_history(self, chat_id, *, from_ms, backward):
        self.calls.append((from_ms, backward))
        return [row for row in self.rows if row["time"] <= from_ms][:backward]


def message(ident, timestamp, text="текст"):
    return {"id": ident, "chatId": 10, "time": timestamp, "sender": 20, "text": text, "attaches": []}


class HistoryPaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_limit_one_keeps_same_timestamp_and_reaches_end(self):
        account = HistoryAccount([message(1, 1000), message(2, 1000), message(3, 900)])
        reader = Messages(account)
        cursor = None
        received = []

        for _ in range(5):
            page = await reader.get_messages(10, limit=1, cursor=cursor, until=2000 if cursor is None else None)
            received.extend(row["message_id"] for row in page["messages"])
            if page["end_reached"]:
                break
            self.assertFalse(page["partial"])
            cursor = page["next_cursor"]

        self.assertEqual(received, ["2", "1", "3"])
        self.assertTrue(page["end_reached"])
        self.assertIn((899, 1), account.calls)

    async def test_seen_boundary_does_not_hide_end_of_limited_range(self):
        account = HistoryAccount([message(1, 10000), message(2, 9000)])
        reader = Messages(account)
        first = await reader.get_messages(10, limit=1, since=10000, until=10000)
        second = await reader.get_messages(10, limit=1, cursor=first["next_cursor"])

        self.assertEqual([row["message_id"] for row in first["messages"]], ["1"])
        self.assertEqual(second["messages"], [])
        self.assertTrue(second["end_reached"])
        self.assertFalse(second["partial"])
        self.assertIsNone(second["next_cursor"])

    async def test_multiple_pages_and_change_of_page_size(self):
        account = HistoryAccount([message(i, 1000 - i) for i in range(1, 10)])
        reader = Messages(account)
        first = await reader.get_messages(10, limit=3, until=2000)
        second = await reader.get_messages(10, limit=2, cursor=first["next_cursor"])
        third = await reader.get_messages(10, limit=4, cursor=second["next_cursor"])
        ids = [row["message_id"] for page in (first, second, third) for row in page["messages"]]
        self.assertEqual(ids, [str(i) for i in range(1, 10)])
        last = await reader.get_messages(10, cursor=third["next_cursor"])
        self.assertTrue(last["end_reached"])

    async def test_full_boundary_reports_stall_without_skipping_messages(self):
        account = HistoryAccount([message(i, 1000) for i in range(201)] + [message(300, 900)])
        reader = Messages(account)
        first = await reader.get_messages(10, limit=200, until=2000)
        second = await reader.get_messages(10, limit=200, cursor=first["next_cursor"])
        self.assertEqual(len(first["messages"]), 200)
        self.assertTrue(second["partial"])
        self.assertEqual(second["reason"], "PAGINATION_STALLED")
        self.assertFalse(second["end_reached"])
        self.assertNotIn((999, 1), account.calls)

    async def test_search_finds_nested_forward_text_without_changing_author_text(self):
        account = HistoryAccount([message(1, 1000, "")])
        reader = Messages(account)
        nested = {"text": "", "forwarded": {"message": {"text": "искомая фраза"}}}
        row = {"message_id": "1", "time_utc": "1970-01-01T00:00:01Z", **nested}

        # Поиск возвращает исходную обёртку, даже если совпадение находится внутри пересылки.
        with patch("max_account.messages.normalize_message", return_value=row):
            result = await reader.search_messages(10, "фраза", scan_limit=1, until=2000)
        self.assertEqual(result["messages"], [row])
        self.assertEqual(result["messages"][0]["text"], "")

    async def test_search_continues_after_page_without_matches(self):
        account = HistoryAccount([message(1, 1200, "первое"), message(2, 1100, "нужное"), message(3, 1000, "нужное")])
        reader = Messages(account)
        first = await reader.search_messages(10, "нужное", scan_limit=1, until=2000)
        second = await reader.search_messages(10, "нужное", scan_limit=2, cursor=first["next_cursor"])
        self.assertEqual(first["messages"], [])
        self.assertEqual([row["message_id"] for row in second["messages"]], ["2", "3"])


if __name__ == "__main__":
    unittest.main()
