"""Подготовка и однократное исполнение исходящего поручения."""

import asyncio
import uuid

from .errors import BridgeError


class Outbox:
    def __init__(self, store, sender, media=None):
        self.store = store
        self.sender = sender
        self.media = media
        self._tasks = set()
        self._pending_recovery = set()
        self._wait_seconds = 5

    # Снимки создаются до записи неизменяемого черновика, их хеши входят в ключ сравнения.
    async def prepare(self, request_key, chat_id, text=None, reply_to=None, attachments=None, batch_id=None):
        attachments = attachments or []
        if not chat_id or (not text and not attachments):
            raise BridgeError("REQUEST_CONFLICT", "Укажите адресата и текст или вложение")
        if attachments and self.media is None:
            raise BridgeError("MEDIA_UNSUPPORTED", "Подготовка вложений недоступна")
        snapshots = await self.media.prepare_attachments(attachments) if attachments else []
        payload = {"chat_id": chat_id, "text": text, "reply_to": reply_to, "attachments": snapshots}
        fingerprint = {
            "chat_id": chat_id, "text": text, "reply_to": reply_to,
            "attachments": [{key: value for key, value in item.items() if key != "file_id"} for item in snapshots],
        }
        try:
            return self.store.prepare(request_key, payload, batch_id, fingerprint=fingerprint)
        finally:
            if snapshots:
                await self.media.remove_unclaimed_snapshots(snapshots)

    def get_status(self, draft_id=None, request_key=None):
        return self.store.get_operation(operation_id=draft_id, request_key=request_key)

    # Отмена запроса клиента не отменяет уже принятый процесс отправки.
    async def send_prepared(self, draft_id, expected_chat_id, user_instruction, authorization_ref=None):
        # При временном отказе базы повторяем восстановление только своих неотправленных захватов.
        self._recover_before_send()
        operation = self.get_status(draft_id=draft_id)
        if operation["payload"]["chat_id"] != expected_chat_id:
            raise BridgeError("REQUEST_CONFLICT", "Адресат не совпадает с подготовленным")
        if not isinstance(user_instruction, str) or not user_instruction.strip():
            raise BridgeError("REQUEST_CONFLICT", "Укажите поручение на отправку")
        claim_token = uuid.uuid4().hex
        try:
            claimed = self.store.claim(draft_id, claim_token)
        except BaseException:
            self._pending_recovery.add((draft_id, claim_token))
            try:
                self._recover_before_send()
            except Exception:
                pass
            raise
        if not claimed:
            return self.get_status(draft_id=draft_id)
        boundary = {"before_send": True}
        task = asyncio.create_task(self._execute(draft_id, claim_token, boundary))
        self._tasks.add(task)
        task.add_done_callback(lambda task: self._finish_task(task, draft_id, claim_token, boundary))
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=self._wait_seconds)
        except TimeoutError:
            return self.get_status(draft_id=draft_id)

    def _finish_task(self, task, draft_id, claim_token, boundary):
        # При отключённом клиенте исключение не должно попасть в журнал среды.
        self._tasks.discard(task)
        if not task.cancelled():
            task.exception()
        if boundary["before_send"]:
            self._pending_recovery.add((draft_id, claim_token))
            try:
                self._recover_before_send()
            except Exception:
                pass

    def _recover_before_send(self):
        for operation_id, claim_token in tuple(self._pending_recovery):
            self.store.recover_before_send(operation_id, claim_token)
            self._pending_recovery.discard((operation_id, claim_token))

    async def _execute(self, draft_id, claim_token, boundary):
        operation = self.get_status(draft_id=draft_id)
        payload = operation["payload"]
        try:
            attachments = await self.media.upload_prepared(payload["attachments"]) if payload["attachments"] else []
        except asyncio.CancelledError:
            self.store.finish(draft_id, "failed_before_send", error={"code": "UPSTREAM_ERROR"})
            raise
        except BridgeError as exc:
            self.store.finish(draft_id, "failed_before_send", error=exc.to_dict())
            raise
        except Exception:
            self.store.finish(draft_id, "failed_before_send", error={"code": "UPSTREAM_ERROR"})
            raise BridgeError("UPSTREAM_ERROR", "Не удалось подготовить вложения") from None

        # Запись sending завершается до первого внешнего вызова отправителя.
        try:
            operation = self.store.mark_sending(draft_id)
        except BaseException:
            self._pending_recovery.add((draft_id, claim_token))
            try:
                self._recover_before_send()
            except Exception:
                pass
            raise
        # После этой точки исход вызова отправителя может быть неизвестным.
        boundary["before_send"] = False
        try:
            receipt = await self.sender(payload, operation["cid"], attachments)
            if not isinstance(receipt, dict) or not receipt:
                raise ValueError("Отправитель не вернул подтверждение")
            return self.store.finish(draft_id, "sent", receipt=receipt)
        except BaseException as exc:
            try:
                self.store.finish(draft_id, "unknown", error={"code": "OUTCOME_UNKNOWN"})
            except Exception:
                # При отказе базы состояние sending будет восстановлено как unknown при запуске.
                pass
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise BridgeError("OUTCOME_UNKNOWN", "Не удалось подтвердить исход отправки") from None

    # История без подтверждённого полного cid не является доказательством отправки.
    async def reconcile(self, draft_id, action="inspect", user_instruction=None):
        operation = self.get_status(draft_id=draft_id)
        if action == "inspect":
            return operation
        if action != "abandon" or not isinstance(user_instruction, str) or not user_instruction.strip():
            raise BridgeError("REQUEST_CONFLICT", "Для прекращения требуется явное поручение")
        return self.store.finish(draft_id, "abandoned")
