import tempfile
import threading
import json
import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from max_account.errors import BridgeError
from max_account.state import StateStore
from max_account.messages import Messages


class StateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "state.db"
        self.first = StateStore(self.path)
        self.second = StateStore(self.path)

    def tearDown(self):
        self.directory.cleanup()

    def record(self, batch_id, chat_id, cursor, end_reached, partial, *, expected_cursor, since, until):
        response = {"messages": [], "next_cursor": cursor, "end_reached": end_reached,
                    "partial": partial, "since": since, "until": until}
        return self.first.record_page(batch_id, chat_id, cursor, end_reached, partial,
                                      expected_cursor=expected_cursor, since=since, until=until, response=response)

    # Независимые соединения должны договориться об одном ключе и одном исполнителе.
    def test_concurrent_prepare_and_claim(self):
        payload = {"chat_id": 1, "text": "один текст"}
        with ThreadPoolExecutor(max_workers=2) as pool:
            operations = list(pool.map(lambda store: store.prepare("key", payload), (self.first, self.second)))
        self.assertEqual(operations[0]["operation_id"], operations[1]["operation_id"])
        operation_id = operations[0]["operation_id"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            claims = list(pool.map(lambda store: store.claim(operation_id), (self.first, self.second)))
        self.assertEqual(sorted(claims), [False, True])
        with self.assertRaises(BridgeError) as error:
            self.second.prepare("key", {"chat_id": 1, "text": "другой текст"})
        self.assertEqual(error.exception.code, "REQUEST_CONFLICT")
        self.assertNotEqual(operation_id, self.first.prepare("new-key", payload)["operation_id"])

    # Закрытие и повторное открытие базы сохраняет границу неизвестного исхода.
    def test_interrupted_send_after_reopen(self):
        first = self.first.prepare("first", {"text": "текст"})
        second = self.first.prepare("second", {"text": "текст"})
        self.first.claim(first["operation_id"])
        sent = self.first.mark_sending(first["operation_id"])
        reopened = StateStore(self.path)
        reopened.recover_interrupted()
        self.assertEqual(reopened.get_operation(operation_id=first["operation_id"])["status"], "unknown")
        self.assertFalse(reopened.claim(first["operation_id"]))
        self.assertTrue(reopened.claim(second["operation_id"]))
        StateStore(self.path).recover_interrupted()
        self.assertEqual(reopened.get_operation(operation_id=second["operation_id"])["status"], "failed_before_send")
        self.assertTrue(reopened.claim(second["operation_id"]))
        another = reopened.mark_sending(second["operation_id"])
        self.assertNotEqual(sent["cid"], another["cid"])

    # Подготовленный черновик после перезапуска остаётся тем же поручением.
    def test_prepared_operation_survives_reopen(self):
        original = self.first.prepare("saved", {"text": "исходный текст"})
        reopened = StateStore(self.path)
        same = reopened.prepare("saved", {"text": "исходный текст"})
        self.assertEqual(original["operation_id"], same["operation_id"])
        self.assertEqual(same["status"], "prepared")

    # Отметка продвигается только после полного чтения и завершения всех действий.
    def test_batch_partial_unknown_and_resume(self):
        batch = self.first.begin_batch("task", [1, 2])
        resumed = self.second.begin_batch("task", [3])
        self.assertEqual(batch["batch_id"], resumed["batch_id"])
        bid = batch["batch_id"]
        self.record(bid, 1, None, True, False, expected_cursor=None, since=None, until=batch["upper_bound"])
        self.record(bid, 2, "next", True, True, expected_cursor=None, since=None, until=batch["upper_bound"])
        restored = StateStore(self.path).get_batch(bid)
        self.assertEqual(restored["pages"][1]["cursor"], "next")
        self.assertEqual(restored["current_checkpoint_version"], 0)
        with self.assertRaises(BridgeError):
            self.first.commit_batch(bid, 0)
        with self.assertRaises(BridgeError):
            self.record(bid, 2, None, True, False, expected_cursor="forged", since=None, until=batch["upper_bound"])
        with self.assertRaises(BridgeError):
            self.record(bid, 3, None, True, False, expected_cursor=None, since=None, until=batch["upper_bound"])
        with self.assertRaises(BridgeError):
            self.record(bid, 2, None, True, False, expected_cursor="next", since=10, until=batch["upper_bound"])
        self.record(bid, 2, None, True, False, expected_cursor="next", since=None, until=batch["upper_bound"])
        op = self.first.prepare("action", {"text": "x"}, bid)
        self.first.claim(op["operation_id"])
        self.first.mark_sending(op["operation_id"])
        self.first.finish(op["operation_id"], "unknown")
        with self.assertRaises(BridgeError):
            self.first.commit_batch(bid, 0)
        self.first.finish(op["operation_id"], "abandoned")
        with self.assertRaises(BridgeError):
            self.first.commit_batch(bid, 1)
        self.first.commit_batch(bid, 0)
        saved = self.second.get_batch(bid)
        self.assertEqual(saved["current_checkpoint_version"], 1)
        self.assertIsNone(saved["pages"][0]["since"])
        next_batch = StateStore(self.path).begin_batch("task", [1, 2])
        self.assertEqual(next_batch["checkpoint_version"], 1)
        self.assertEqual(next_batch["pages"][0]["since"], batch["upper_bound"])

    # Передача сохраняет ключ при перезапуске и блокирует фиксацию до завершения.
    def test_transfer_survives_reopen_and_blocks_batch(self):
        batch = self.first.begin_batch("download", [1])
        bid = batch["batch_id"]
        self.record(bid, 1, None, True, False, expected_cursor=None, since=None, until=batch["upper_bound"])
        transfer, created = self.first.claim_transfer("file", {"source": "1"}, bid)
        self.assertTrue(created)
        reopened = StateStore(self.path)
        same, created = reopened.claim_transfer("file", {"source": "1"}, bid)
        self.assertFalse(created)
        self.assertEqual(same["transfer_id"], transfer["transfer_id"])
        with self.assertRaises(BridgeError):
            reopened.commit_batch(bid, 0)
        reopened.save_asset({"file_id": "file", "status": "ready"})
        reopened.update_transfer(transfer["transfer_id"], {"status": "completed", "result": {"file_id": "file"}})
        reopened.commit_batch(bid, 0)

    def test_invalid_transfer_key(self):
        for key in ("", " ", "x" * 161):
            with self.assertRaises(BridgeError):
                self.first.claim_transfer(key, {"source": "1"})

    # Повтор после потери ответа и гонка клиентов возвращают именно сохранённую страницу.
    def test_recorded_pages_survive_disconnect_and_reopen(self):
        batch = self.first.begin_batch("history", [1])
        bid, upper = batch["batch_id"], batch["upper_bound"]
        first_page = {"messages": [{"id": "a"}], "next_cursor": "next", "end_reached": False,
                      "partial": False, "since": None, "until": upper}
        other_page = {**first_page, "messages": [{"id": "other"}]}

        def save(store, page):
            return store.record_page(bid, 1, "next", False, False, expected_cursor=None,
                                     since=None, until=upper, response=page)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda args: save(*args), [(self.first, first_page), (self.second, other_page)]))
        self.assertEqual(results[0], results[1])
        reopened = StateStore(self.path)
        self.assertEqual(reopened.get_recorded_page(bid, 1, None), results[0])
        self.assertEqual(reopened.get_batch(bid)["pages"][0]["cursor"], "next")
        self.assertIsNone(reopened.get_recorded_page(bid, 1, "next"))
        last = {"messages": [{"id": "b"}], "next_cursor": None, "end_reached": True,
                "partial": False, "since": None, "until": upper}
        reopened.record_page(bid, 1, None, True, False, expected_cursor="next", since=None, until=upper, response=last)
        reopened.commit_batch(bid, 0)
        self.assertEqual(StateStore(self.path).get_recorded_page(bid, 1, "next"), last)

    # Сохраняются два последних ответа; старый курсор не продвигает открытый пакет.
    def test_page_history_is_bounded_and_expired_after_commit(self):
        with patch("max_account.state.time.time", return_value=1000):
            batch = self.first.begin_batch("retention", [1])
            bid, upper = batch["batch_id"], batch["upper_bound"]
            for current, following in ((None, "one"), ("one", "two"), ("two", "three")):
                self.record(bid, 1, following, False, False, expected_cursor=current, since=None, until=upper)
        self.assertIsNone(self.first.get_recorded_page(bid, 1, None))
        self.assertIsNotNone(self.first.get_recorded_page(bid, 1, "one"))
        self.assertIsNotNone(self.first.get_recorded_page(bid, 1, "two"))
        with self.assertRaises(BridgeError):
            self.record(bid, 1, "one", False, False, expected_cursor=None, since=None, until=upper)
        with patch("max_account.state.time.time", return_value=1000 + 25 * 3600):
            self.assertIsNotNone(StateStore(self.path).get_recorded_page(bid, 1, "two"))
            self.record(bid, 1, None, True, False, expected_cursor="three", since=None, until=upper)
            self.first.commit_batch(bid, 0)
        with patch("max_account.state.time.time", return_value=1000 + 48 * 3600):
            self.assertIsNotNone(self.first.get_recorded_page(bid, 1, "three"))
        with patch("max_account.state.time.time", return_value=1000 + 49 * 3600):
            self.assertIsNone(StateStore(self.path).get_recorded_page(bid, 1, "three"))

    # Превышение общего предела откатывает запись и курсор, сохраняя прежнюю страницу.
    def test_response_budget_never_advances_cursor(self):
        batch = self.first.begin_batch("budget", [1])
        bid, upper = batch["batch_id"], batch["upper_bound"]
        self.record(bid, 1, "one", False, False, expected_cursor=None, since=None, until=upper)
        with patch("max_account.state.MAX_BATCH_RESPONSE_BYTES", 1):
            with self.assertRaises(BridgeError) as error:
                self.record(bid, 1, "two", False, False, expected_cursor="one", since=None, until=upper)
        self.assertEqual(error.exception.code, "RESOURCE_LIMIT")
        self.assertFalse(error.exception.retryable)
        self.assertEqual(self.first.get_batch(bid)["pages"][0]["cursor"], "one")
        self.assertIsNotNone(self.first.get_recorded_page(bid, 1, None))
        self.assertIsNone(self.first.get_recorded_page(bid, 1, "one"))

    # Отказ освобождает место без изменения квитанций и отметки задания.
    def test_abandon_frees_budget_without_advancing_checkpoint(self):
        batch = self.first.begin_batch("aborted", [1])
        other = self.first.begin_batch("other", [2])
        op = self.first.prepare("send", {"text": "доставлено"}, batch["batch_id"])
        self.first.claim(op["operation_id"])
        self.first.mark_sending(op["operation_id"])
        sent = self.first.finish(op["operation_id"], "sent", receipt={"id": "receipt"})

        def page(current_batch):
            return {"messages": [{"text": "данные" * 25}], "next_cursor": "next",
                    "end_reached": False, "partial": False, "since": None,
                    "until": current_batch["upper_bound"]}

        def write(current_batch, chat_id):
            response = page(current_batch)
            return self.first.record_page(current_batch["batch_id"], chat_id, "next", False, False,
                                          expected_cursor=None, since=None, until=response["until"], response=response)

        first_size = len(json.dumps(page(batch), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        second_size = len(json.dumps(page(other), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        with patch("max_account.state.MAX_BATCH_RESPONSE_BYTES", first_size + second_size - 1):
            write(batch, 1)
            with self.assertRaises(BridgeError) as error:
                write(other, 2)
            self.assertEqual(error.exception.code, "RESOURCE_LIMIT")
            abandoned = self.first.abandon_batch(batch["batch_id"])
            self.assertEqual(abandoned["status"], "abandoned")
            self.assertEqual(self.second.abandon_batch(batch["batch_id"])["status"], "abandoned")
            self.assertIsNone(self.first.get_recorded_page(batch["batch_id"], 1, None))
            self.assertEqual(self.first.get_operation(operation_id=op["operation_id"]), sent)
            self.assertEqual(write(other, 2), page(other))
        restarted = self.first.begin_batch("aborted", [1])
        self.assertNotEqual(restarted["batch_id"], batch["batch_id"])
        self.assertEqual(restarted["checkpoint_version"], batch["checkpoint_version"])
        self.assertEqual(restarted["pages"][0]["since"], batch["pages"][0]["since"])

    # Активный исполнитель не должен потерять пакет во время внешней операции.
    def test_abandon_waits_for_active_actions(self):
        batch = self.first.begin_batch("active", [1])
        bid = batch["batch_id"]
        op = self.first.prepare("pending-send", {"text": "x"}, bid)
        self.first.claim(op["operation_id"])
        with self.assertRaises(BridgeError) as error:
            self.second.abandon_batch(bid)
        self.assertEqual(error.exception.code, "BUSY")
        self.first.finish(op["operation_id"], "failed_before_send")
        transfer, _ = self.first.claim_transfer("pending-file", {"source": "1"}, bid)
        with self.assertRaises(BridgeError) as error:
            self.second.abandon_batch(bid)
        self.assertEqual(error.exception.code, "BUSY")
        self.first.update_transfer(transfer["transfer_id"], {"status": "failed"})
        self.assertEqual(self.second.abandon_batch(bid)["status"], "abandoned")
        with self.assertRaises(BridgeError) as error:
            self.first.claim(op["operation_id"])
        self.assertEqual(error.exception.code, "CHECKPOINT_CONFLICT")
        self.assertEqual(self.first.get_operation(operation_id=op["operation_id"])["status"], "failed_before_send")

    def test_interrupted_transfer_is_not_restarted_by_old_key(self):
        transfer, created = self.first.claim_transfer("interrupted", {"source": "1"})
        self.assertTrue(created)
        self.first.update_transfer(transfer["transfer_id"], {"status": "downloading"})
        reopened = StateStore(self.path)
        reopened.recover_interrupted()
        current, created = reopened.claim_transfer("interrupted", {"source": "1"})
        self.assertFalse(created)
        self.assertEqual(current["status"], "failed")
        self.assertEqual(current["error"]["code"], "UPSTREAM_ERROR")

    # Снимок, связанный с черновиком, нельзя удалить при параллельной очистке.
    def test_asset_claim_and_cleanup_are_atomic(self):
        asset = {"file_id": "snapshot", "status": "ready", "path": "private"}
        self.first.save_asset(asset)
        self.first.prepare("draft", {"attachments": [{"file_id": "snapshot"}]})
        self.assertEqual(self.second.claim_unclaimed_assets(["snapshot"]), [])
        self.assertEqual(self.second.get_asset(file_id="snapshot"), asset)
        spare = {"file_id": "spare", "status": "ready", "path": "private2"}
        self.first.save_asset(spare)
        self.assertEqual(self.second.claim_unclaimed_assets(["spare"]), [spare])
        self.second.finish_asset_deletion("spare", deleted=True)
        with self.assertRaises(BridgeError):
            self.first.prepare("another", {"attachments": [{"file_id": "spare"}]})

        self.first.save_asset({"file_id": "race", "status": "ready"})
        barrier = threading.Barrier(2)

        def claim():
            barrier.wait()
            try:
                return self.first.prepare("raced", {"attachments": [{"file_id": "race"}]})
            except BridgeError as error:
                return error.code

        def discard():
            barrier.wait()
            return self.second.claim_unclaimed_assets(["race"])

        with ThreadPoolExecutor(max_workers=2) as pool:
            claimed = pool.submit(claim)
            discarded = pool.submit(discard)
            result, removed = claimed.result(), discarded.result()
        if isinstance(result, dict):
            self.assertEqual(removed, [])
            self.assertIsNotNone(self.first.get_asset(file_id="race"))
        else:
            self.assertEqual(result, "FILE_EXPIRED")
            self.assertEqual(len(removed), 1)

    # Неизвестный исход и открытый пакет защищают файлы; квитанция переживает очистку.
    def test_expired_asset_recovery_and_protected_operations(self):
        def asset(file_id, **extra):
            return {"file_id": file_id, "status": "ready", "created_at": 10, **extra}

        self.first.save_asset(asset("uncertain"))
        unknown = self.first.prepare("uncertain", {"attachments": [{"file_id": "uncertain"}]})
        self.first.claim(unknown["operation_id"])
        self.first.mark_sending(unknown["operation_id"])
        self.first.finish(unknown["operation_id"], "unknown")

        self.first.save_asset(asset("sent"))
        sent = self.first.prepare("sent", {"attachments": [{"file_id": "sent"}]})
        self.first.claim(sent["operation_id"])
        self.first.mark_sending(sent["operation_id"])
        sent = self.first.finish(sent["operation_id"], "sent", receipt={"id": "receipt"})

        batch = self.first.begin_batch("open", [1])
        self.first.save_asset(asset("open", batch_id=batch["batch_id"]))
        self.first.save_asset(asset("transfer"))
        transfer, _ = self.first.claim_transfer("transfer", {"source": "x"}, batch["batch_id"])
        self.first.update_transfer(transfer["transfer_id"], {"status": "ready", "result": {"file_id": "transfer"}})
        self.assertEqual([a["file_id"] for a in self.first.claim_expired_assets(20)], ["sent"])
        self.assertIsNone(self.first.get_asset(file_id="sent"))
        self.assertEqual(self.second.claim_expired_assets(20), [])
        reopened = StateStore(self.path)
        reopened.recover_asset_deletions()
        self.assertEqual([a["file_id"] for a in reopened.claim_expired_assets(20)], ["sent"])
        reopened.finish_asset_deletion("sent", deleted=False)
        self.assertIsNotNone(reopened.get_asset(file_id="sent"))
        reopened.claim_expired_assets(20)
        reopened.finish_asset_deletion("sent", deleted=True)
        self.assertIsNone(reopened.get_asset(file_id="sent"))
        self.assertEqual(reopened.get_operation(operation_id=sent["operation_id"]), sent)
        self.assertIsNotNone(reopened.get_asset(file_id="uncertain"))
        self.assertIsNotNone(reopened.get_asset(file_id="open"))
        self.assertIsNotNone(reopened.get_asset(file_id="transfer"))

    # Гонка очистки и подготовки не даёт удалить файл, уже привязанный к черновику.
    def test_expired_claim_races_prepare(self):
        self.first.save_asset({"file_id": "race-expired", "status": "ready", "created_at": 10})
        barrier = threading.Barrier(2)

        def prepare():
            barrier.wait()
            try:
                return self.first.prepare("race-expired", {"attachments": [{"file_id": "race-expired"}]})
            except BridgeError as error:
                return error.code

        def clean():
            barrier.wait()
            return self.second.claim_expired_assets(20)

        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = pool.submit(prepare), pool.submit(clean)
            operation, claimed = first.result(), second.result()
        if isinstance(operation, dict):
            self.assertEqual(claimed, [])
        else:
            self.assertEqual(operation, "FILE_EXPIRED")
            self.assertEqual(len(claimed), 1)

    # Неудачное удаление одного файла не мешает обработать остальные.
    def test_cleanup_failure_does_not_starve_other_files(self):
        for file_id in ("first", "second"):
            self.first.save_asset({"file_id": file_id, "status": "ready", "created_at": 10})
        claimed = self.first.claim_expired_assets(20, limit=1)
        self.assertEqual(claimed[0]["file_id"], "first")
        self.first.finish_asset_deletion("first", deleted=False)
        self.assertEqual(self.first.claim_expired_assets(20, limit=1)[0]["file_id"], "second")

    # Подменённый файл сохраняется для проверки и не попадает в бесконечный цикл очистки.
    def test_preserved_asset_stays_visible_but_not_reclaimed(self):
        self.first.save_asset({"file_id": "changed", "status": "ready", "created_at": 10})
        self.first.claim_expired_assets(20)
        self.first.finish_asset_deletion("changed", deleted=False, preserved=True)
        reopened = StateStore(self.path)
        reopened.recover_asset_deletions()
        self.assertIsNotNone(reopened.get_asset(file_id="changed"))
        self.assertEqual(reopened.claim_expired_assets(20), [])

    # Существующая база получает поле попытки очистки без потери записей.
    def test_old_asset_schema_migrates(self):
        path = Path(self.directory.name) / "old.db"
        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE assets (file_id TEXT PRIMARY KEY, request_key TEXT UNIQUE, batch_id TEXT, status TEXT, data TEXT NOT NULL)")
            db.execute("INSERT INTO assets VALUES (?,?,?,?,?)", ("old", None, None, "ready",
                       json.dumps({"file_id": "old", "status": "ready", "created_at": 10})))
        db.close()
        migrated = StateStore(path)
        self.assertEqual(migrated.claim_expired_assets(20)[0]["file_id"], "old")
        self.assertIsNone(migrated.get_asset(file_id="old"))
        migrated.recover_asset_deletions()
        self.assertIsNotNone(migrated.get_asset(file_id="old"))


class BatchHistoryTests(unittest.IsolatedAsyncioTestCase):
    # Граница пакета и запрос истории используют одну шкалу миллисекунд, а не 1970 год.
    async def test_batch_boundary_reaches_message_in_2026(self):
        class Account:
            account_id = "owner"

            def __init__(self):
                self.from_values = []

            async def get_history(self, chat_id, from_ms, backward):
                self.from_values.append(from_ms)
                return [{"id": 123, "time": 1793145600000, "text": "сообщение"}] if len(self.from_values) == 1 else []

        with tempfile.TemporaryDirectory() as directory:
            store = StateStore(Path(directory) / "state.db")
            with patch("max_account.state.time.time_ns", return_value=1793232000000 * 1_000_000):
                batch = store.begin_batch("task", [1], "2026-10-27T00:00:00Z")
            self.assertEqual(batch["upper_bound"], 1793232000000)
            self.assertIsInstance(batch["pages"][0]["since"], int)
            account = Account()
            reader = Messages(account)
            page = await reader.get_messages(1, since=batch["pages"][0]["since"], until=batch["upper_bound"])
            self.assertEqual(account.from_values[0], 1793232000000)
            self.assertEqual(page["messages"][0]["text"], "сообщение")
            store.record_page(batch["batch_id"], 1, page["next_cursor"], page["end_reached"], page["partial"],
                              expected_cursor=None, since=page["since"], until=page["until"], response=page)
            with self.assertRaises(BridgeError):
                store.commit_batch(batch["batch_id"], batch["checkpoint_version"])


if __name__ == "__main__":
    unittest.main()
