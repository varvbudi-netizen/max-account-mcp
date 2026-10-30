import asyncio
import tempfile
import unittest
from pathlib import Path

from max_account.errors import BridgeError
from max_account.outbox import Outbox
from max_account.state import StateStore


class OutboxTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "state.db"
        self.store = StateStore(self.path)

    async def asyncTearDown(self):
        self.directory.cleanup()

    # Два клиента делят единственный захват и не меняют исходный текст.
    async def test_concurrent_send_and_distinct_intentions(self):
        calls = []
        entered = asyncio.Event()
        release = asyncio.Event()

        async def sender(payload, cid, attachments):
            calls.append((payload, cid, attachments))
            entered.set()
            await release.wait()
            return {"message_id": "confirmed"}

        first = Outbox(self.store, sender)
        other = Outbox(StateStore(self.path), sender)
        text = "*звёздочки* _подчёркивания_\nhttps://example.org"
        draft = await first.prepare("one", 17, text)
        task = asyncio.create_task(first.send_prepared(draft["draft_id"], 17, "отправить"))
        await entered.wait()
        concurrent = await other.send_prepared(draft["draft_id"], 17, "отправить")
        self.assertEqual(concurrent["status"], "sending")
        release.set()
        self.assertEqual((await task)["status"], "sent")
        self.assertEqual(calls[0][0]["text"], text)
        self.assertEqual(len(calls), 1)
        another = await first.prepare("two", 17, text)
        await first.send_prepared(another["draft_id"], 17, "отправить ещё раз")
        self.assertEqual(len(calls), 2)
        self.assertNotEqual(calls[0][1], calls[1][1])
        reopened = StateStore(self.path)
        self.assertEqual(reopened.get_operation(request_key="one")["receipt"]["message_id"], "confirmed")

    # Потеря ответа после сетевой границы не разрешает второй вызов отправителя.
    async def test_lost_response_is_unknown(self):
        calls = []

        async def sender(payload, cid, attachments):
            calls.append(cid)
            raise ConnectionError("ответ потерян")

        outbox = Outbox(self.store, sender)
        draft = await outbox.prepare("one", 1, "текст")
        with self.assertRaises(BridgeError) as error:
            await outbox.send_prepared(draft["draft_id"], 1, "отправить")
        self.assertEqual(error.exception.code, "OUTCOME_UNKNOWN")
        reopened = StateStore(self.path)
        reopened.recover_interrupted()
        result = await Outbox(reopened, sender).send_prepared(draft["draft_id"], 1, "отправить")
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(len(calls), 1)

    # Разрыв клиента не прерывает принятую отправку и сохранение квитанции.
    async def test_client_cancellation_does_not_cancel_sender(self):
        started = asyncio.Event()
        release = asyncio.Event()
        done = asyncio.Event()

        async def sender(payload, cid, attachments):
            started.set()
            await release.wait()
            done.set()
            return {"message_id": "accepted"}

        outbox = Outbox(self.store, sender)
        draft = await outbox.prepare("cancel", 1, "текст")
        request = asyncio.create_task(outbox.send_prepared(draft["draft_id"], 1, "отправить"))
        await started.wait()
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        release.set()
        await done.wait()
        await asyncio.gather(*outbox._tasks)
        self.assertEqual(self.store.get_operation(operation_id=draft["draft_id"])["status"], "sent")

    # Разные операции не создают очередь и не выполняются одновременно.
    async def test_global_limit_and_bounded_wait(self):
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def sender(payload, cid, attachments):
            calls.append(cid)
            started.set()
            await release.wait()
            return {"message_id": "accepted"}

        first = Outbox(self.store, sender)
        second = Outbox(StateStore(self.path), sender)
        first._wait_seconds = 0.01
        draft_one = await first.prepare("first", 1, "текст")
        draft_two = await second.prepare("second", 1, "другой текст")
        running = asyncio.create_task(first.send_prepared(draft_one["draft_id"], 1, "отправить"))
        await started.wait()
        with self.assertRaises(BridgeError) as error:
            await second.send_prepared(draft_two["draft_id"], 1, "отправить")
        self.assertEqual(error.exception.code, "BUSY")
        self.assertEqual((await running)["status"], "sending")
        self.assertEqual(self.store.get_operation(operation_id=draft_two["draft_id"])["status"], "prepared")
        release.set()
        await asyncio.gather(*first._tasks)
        self.assertEqual((await second.send_prepared(draft_two["draft_id"], 1, "отправить"))["status"], "sent")
        self.assertEqual(len(calls), 2)

    # Повтор подготовки очищает лишний снимок, сохраняя привязанный к черновику.
    async def test_repeated_prepare_cleans_only_unclaimed_snapshots(self):
        store = self.store
        directory = Path(self.directory.name)

        class Media:
            def __init__(self):
                self.counter = 0

            async def prepare_attachments(self, attachments):
                self.counter += 1
                file_id = str(self.counter)
                path = directory / file_id
                path.write_bytes(b"data")
                asset = {"file_id": file_id, "path": str(path), "status": "ready",
                         "sha256": "hash", "size": 4, "kind": "document", "filename": "a"}
                store.save_asset(asset)
                return [{key: value for key, value in asset.items() if key not in ("path", "status")}]

            async def remove_unclaimed_snapshots(self, assets):
                for asset in store.claim_unclaimed_assets([a["file_id"] for a in assets]):
                    Path(asset["path"]).unlink()
                    store.finish_asset_deletion(asset["file_id"], deleted=True)

        outbox = Outbox(store, None, Media())
        original = await outbox.prepare("same", 1, "text", attachments=[{"path": "source"}])
        repeated = await outbox.prepare("same", 1, "text", attachments=[{"path": "source"}])
        self.assertEqual(original["draft_id"], repeated["draft_id"])
        with self.assertRaises(BridgeError):
            await outbox.prepare("same", 1, "different", attachments=[{"path": "source"}])
        self.assertTrue((directory / "1").exists())
        self.assertFalse((directory / "2").exists())
        self.assertFalse((directory / "3").exists())


if __name__ == "__main__":
    unittest.main()
