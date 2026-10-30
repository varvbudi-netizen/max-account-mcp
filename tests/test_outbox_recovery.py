import asyncio
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from max_account.errors import BridgeError
from max_account.outbox import Outbox
from max_account.state import StateStore


class OutboxRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "state.db"
        self.store = StateStore(self.path)
        self.calls = []

        async def sender(payload, cid, attachments):
            self.calls.append((payload, cid))
            return {"message_id": "accepted", "cid": cid}

        self.outbox = Outbox(self.store, sender)

    async def asyncTearDown(self):
        self.directory.cleanup()

    # Существующая база сохраняет историю, отметку и файл при обоих открытиях.
    async def test_old_database_migrates_without_losing_state(self):
        path = Path(self.directory.name) / "old.db"
        payload = {"chat_id": 1, "text": "текст", "attachments": []}
        receipt = {"message_id": "confirmed", "cid": "101"}
        asset = {"file_id": "file-1", "status": "ready", "path": "saved-file", "size": 8}

        with closing(sqlite3.connect(path)) as db, db:
            db.executescript("""
                CREATE TABLE operations (
                    operation_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE,
                    payload TEXT NOT NULL, fingerprint TEXT NOT NULL, batch_id TEXT, cid INTEGER UNIQUE,
                    status TEXT NOT NULL, receipt TEXT, error TEXT
                );
                CREATE TABLE checkpoints (
                    task_key TEXT NOT NULL, chat_id TEXT NOT NULL, version INTEGER NOT NULL,
                    since INTEGER NOT NULL, PRIMARY KEY(task_key, chat_id)
                );
                CREATE TABLE assets (
                    file_id TEXT PRIMARY KEY, request_key TEXT UNIQUE, batch_id TEXT,
                    status TEXT, data TEXT NOT NULL, cleanup_attempt REAL
                );
            """)
            encoded = json.dumps(payload)
            db.executemany("INSERT INTO operations VALUES (?, ?, ?, ?, NULL, ?, ?, ?, NULL)", [
                ("sent-id", "sent-key", encoded, encoded, 101, "sent", json.dumps(receipt)),
                ("unknown-id", "unknown-key", encoded, encoded, 102, "unknown", None),
            ])
            db.execute("INSERT INTO checkpoints VALUES (?, ?, ?, ?)", ("task", "1", 3, 123456))
            db.execute("INSERT INTO assets VALUES (?, NULL, NULL, ?, ?, NULL)",
                       (asset["file_id"], "ready", json.dumps(asset)))

        for _ in range(2):
            reopened = StateStore(path)
            sent = reopened.get_operation(request_key="sent-key")
            unknown = reopened.get_operation(request_key="unknown-key")
            self.assertEqual((sent["status"], sent["cid"], sent["receipt"], sent["payload"]),
                             ("sent", 101, receipt, payload))
            self.assertEqual((unknown["status"], unknown["cid"], unknown["receipt"]),
                             ("unknown", 102, None))
            self.assertNotIn("claim_token", json.dumps([sent, unknown]))
            self.assertEqual(reopened.get_asset(file_id="file-1"), asset)
            active = reopened.prepare("active-key", payload)
            if active["status"] == "prepared":
                self.assertTrue(reopened.claim(active["operation_id"], "private-token"))
            active = reopened.get_operation(request_key="active-key")
            self.assertNotIn("private-token", json.dumps(active))
            self.assertNotIn("claim_token", active)
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual(db.execute("SELECT version, since FROM checkpoints WHERE task_key='task' AND chat_id='1'").fetchone(),
                                 (3, 123456))
                self.assertIn("claim_token", {row[1] for row in db.execute("PRAGMA table_info(operations)")})

    # Проверяем сбой до записи и потерю подтверждения после успешной записи.
    async def test_mark_failure_before_and_after_commit(self):
        original = self.store.mark_sending
        for after_commit in (False, True):
            draft = await self.outbox.prepare(f"key-{after_commit}", 1, "текст")
            committed_cid = []

            def fail(operation_id):
                if after_commit:
                    committed_cid.append(original(operation_id)["cid"])
                raise sqlite3.OperationalError("ошибка базы")

            self.store.mark_sending = fail
            with self.assertRaises(sqlite3.OperationalError):
                await self.outbox.send_prepared(draft["draft_id"], 1, "отправить")
            self.assertEqual(self.store.get_operation(operation_id=draft["draft_id"])["status"], "failed_before_send")
            self.assertEqual(len(self.calls), int(after_commit))
            self.store.mark_sending = original
            receipt = await self.outbox.send_prepared(draft["draft_id"], 1, "отправить")
            self.assertEqual(receipt["status"], "sent")
            if after_commit:
                self.assertEqual(receipt["cid"], committed_cid[0])
            repeat = await self.outbox.prepare(f"key-{after_commit}", 1, "текст")
            self.assertEqual(repeat["receipt"], receipt["receipt"])
        self.assertEqual(len(self.calls), 2)

    # Восстановление откладывается, пока база недоступна, и не требует перезапуска.
    async def test_recovery_retries_when_database_becomes_available(self):
        first = await self.outbox.prepare("first", 1, "первый")
        second = await self.outbox.prepare("second", 1, "второй")
        mark = self.store.mark_sending
        recover = self.store.recover_before_send

        def fail(*args):
            raise sqlite3.OperationalError("ошибка базы")

        self.store.mark_sending = fail
        self.store.recover_before_send = fail
        with self.assertRaises(sqlite3.OperationalError):
            await self.outbox.send_prepared(first["draft_id"], 1, "отправить")
        await asyncio.sleep(0)
        self.assertEqual(self.store.get_operation(operation_id=first["draft_id"])["status"], "preparing")
        self.store.mark_sending = mark
        self.store.recover_before_send = recover
        self.assertEqual((await self.outbox.send_prepared(second["draft_id"], 1, "отправить"))["status"], "sent")
        self.assertEqual(self.store.get_operation(operation_id=first["draft_id"])["status"], "failed_before_send")
        self.assertEqual((await self.outbox.send_prepared(first["draft_id"], 1, "отправить"))["status"], "sent")
        self.assertEqual(len(self.calls), 2)

    # Ошибка после подтверждения захвата тоже не оставляет вечную блокировку.
    async def test_claim_error_after_commit(self):
        draft = await self.outbox.prepare("claim", 1, "текст")
        original = self.store.claim

        def fail_after_commit(operation_id, claim_token):
            original(operation_id, claim_token)
            raise sqlite3.OperationalError("подтверждение потеряно")

        self.store.claim = fail_after_commit
        with self.assertRaises(sqlite3.OperationalError):
            await self.outbox.send_prepared(draft["draft_id"], 1, "отправить")
        self.assertEqual(self.store.get_operation(operation_id=draft["draft_id"])["status"], "failed_before_send")
        self.store.claim = original
        self.assertEqual((await self.outbox.send_prepared(draft["draft_id"], 1, "отправить"))["status"], "sent")
        self.assertEqual(len(self.calls), 1)

    # Старое восстановление не может снять новый захват другой задачи.
    async def test_stale_recovery_does_not_release_another_claim(self):
        draft = await self.outbox.prepare("race", 1, "текст")
        self.assertTrue(self.store.claim(draft["draft_id"], "old"))
        self.assertTrue(self.store.recover_before_send(draft["draft_id"], "old"))
        other = StateStore(self.path)
        self.assertTrue(other.claim(draft["draft_id"], "new"))
        self.assertFalse(self.store.recover_before_send(draft["draft_id"], "old"))
        self.assertEqual(other.get_operation(operation_id=draft["draft_id"])["status"], "preparing")

    # Отмена фоновой задачи до её старта освобождает собственный захват.
    async def test_cancel_before_background_start(self):
        draft = await self.outbox.prepare("cancel", 1, "текст")
        self.assertTrue(self.store.claim(draft["draft_id"], "owner"))
        boundary = {"before_send": True}
        task = asyncio.create_task(self.outbox._execute(draft["draft_id"], "owner", boundary))
        self.outbox._tasks.add(task)
        task.add_done_callback(lambda done: self.outbox._finish_task(done, draft["draft_id"], "owner", boundary))
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)
        self.assertEqual(self.store.get_operation(operation_id=draft["draft_id"])["status"], "failed_before_send")
        self.assertEqual((await self.outbox.send_prepared(draft["draft_id"], 1, "отправить"))["status"], "sent")
        self.assertEqual(len(self.calls), 1)

    # После вызова отправителя потерянный ответ остаётся неизвестным и не повторяется.
    async def test_lost_response_after_restart_is_never_retried(self):
        async def lose_response(payload, cid, attachments):
            self.calls.append((payload, cid))
            raise ConnectionError("ответ потерян")

        outbox = Outbox(self.store, lose_response)
        draft = await outbox.prepare("unknown", 1, "текст")
        with self.assertRaises(BridgeError):
            await outbox.send_prepared(draft["draft_id"], 1, "отправить")
        reopened = StateStore(self.path)
        reopened.recover_interrupted()
        result = await Outbox(reopened, lose_response).send_prepared(draft["draft_id"], 1, "отправить")
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(len(self.calls), 1)
