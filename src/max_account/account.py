from __future__ import annotations

import asyncio
import json
from urllib.parse import parse_qs, quote, urlsplit

from .errors import BridgeError


# Служба не запрашивает у пользователя код или пароль.
class NoInteractiveAuthFlow:
    async def authenticate(self, app):
        raise BridgeError("AUTH_REQUIRED", "Требуется локальный вход владельца в MAX")


# Сетевые журналы не передают содержимое запросов в общий журнал приложения.
def silence_network_logs():
    import logging

    for name in ("pymax", "websockets", "aiohttp", "httpcore", "httpx", "httpx2", "mcp"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.addHandler(logging.NullHandler())
        logger.propagate = False


def make_client(config, interactive=False):
    # Конструктор SDK перенастраивает свой журнал, поэтому закрываем его до и после вызова.
    silence_network_logs()
    from pymax import WebClient
    from pymax.config import ExtraConfig
    from pymax.connection import ConnectionManager
    from pymax.connection.readers import WSReader
    from pymax.protocol.tcp import TcpProtocol
    from pymax.transport.websocket import WebSocketTransport
    from websockets.asyncio import client
    from websockets.typing import Origin

    # Защищаем содержимое кадров и исключаем системный прокси.
    class SafeTransport(WebSocketTransport):
        async def connect(self):
            self.ws = await client.connect(
                self.url, origin=Origin("https://web.max.ru"),
                proxy=None, max_size=10 * 1024 * 1024,
            )

        async def send(self, data):
            if self.ws is None or not self.connected:
                raise ConnectionError("Соединение с MAX закрыто")
            await self.ws.send(data)

    class SafeClient(WebClient):
        def _build_connection(self):
            transport = SafeTransport(url=self.extra_config.url, proxy=None)
            return ConnectionManager(
                reader=WSReader(transport), transport=transport, protocol=TcpProtocol(),
            )

    settings = ExtraConfig(
        proxy=None, telemetry=False, relogin=False, reconnect=False,
        request_timeout=config.request_timeout, upload_timeout=config.upload_timeout,
        log_level="INFO",
    )
    try:
        return SafeClient(
            work_dir=str(config.session_dir), extra_config=settings,
            auth_flow=None if interactive else NoInteractiveAuthFlow(),
        )
    finally:
        silence_network_logs()


class Adapter:
    def __init__(self, config, interactive=False):
        self.config = config
        self.interactive = interactive
        self.client = None
        self.account_id = None
        self._ready = False
        self._uploads = {}
        self._monitor = None
        self._connect_lock = asyncio.Lock()
        self._stopping = False
        self._last_error = "AUTH_REQUIRED"

    # Проверяем сохранённый вход до сетевого подключения.
    async def _connect_once(self):
        async with self._connect_lock:
            if self._ready and self.client and self.client._app.connection.is_open:
                return
            self._ready = False
            if self.client is not None:
                await self.client.close()
            self.client = None
            try:
                self.config.session_dir.mkdir(parents=True, exist_ok=True)
                self.client = make_client(self.config, interactive=self.interactive)
                await self.client._ensure_runtime()
                app = self.client._app
                if not self.interactive:
                    if await app.store.load_session() is None:
                        raise BridgeError("AUTH_REQUIRED", "Требуется локальный вход владельца в MAX")
                    if self.config.expected_account_id is None:
                        raise BridgeError("ACCOUNT_MISMATCH", "Аккаунт ещё не закреплён за службой")
                await app.start()
                account_id = str(app.me.contact.id)
                expected = self.config.expected_account_id
                if expected is not None and str(expected) != account_id:
                    raise BridgeError("ACCOUNT_MISMATCH", "Аккаунт MAX не совпадает с сохранённым")
                self.account_id = account_id
                self._ready = True
                self._last_error = None
            except BaseException as exc:
                app = getattr(self.client, "_BaseClient__app", None)
                invalid_token = app is not None and app._is_invalid_login_token_error(exc)
                if self.client is not None:
                    await self.client.close()
                self._ready = False
                if isinstance(exc, asyncio.CancelledError):
                    raise
                if isinstance(exc, BridgeError):
                    self._last_error = exc.code
                    raise
                if invalid_token:
                    self._last_error = "AUTH_REQUIRED"
                    raise BridgeError("AUTH_REQUIRED", "Требуется повторный локальный вход в MAX") from None
                self._last_error = "UPSTREAM_ERROR"
                raise BridgeError("UPSTREAM_ERROR", "Не удалось подключиться к MAX", retryable=True) from None

    # Восстанавливаем сеанс после разрыва, не повторяя отправку.
    # Ограничиваем сетевую попытку; отдельный интерактивный вход ждёт владельца.
    async def _bounded_connect(self):
        if self.interactive:
            return await self._connect_once()
        try:
            return await asyncio.wait_for(
                self._connect_once(), timeout=getattr(self.config, "request_timeout", 30),
            )
        except TimeoutError:
            self._last_error = "UPSTREAM_ERROR"
            raise BridgeError("UPSTREAM_ERROR", "Превышено время подключения к MAX", retryable=True) from None

    async def _watch_connection(self):
        delay = 5
        while not self._stopping:
            if self._ready:
                try:
                    await self.client._app.connection.wait_closed()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                if self._stopping:
                    break
                self._ready = False
                self._last_error = "UPSTREAM_ERROR"
            await asyncio.sleep(delay)
            if self._stopping:
                break
            try:
                await self._bounded_connect()
                delay = 5
            except BridgeError as exc:
                if exc.code == "ACCOUNT_MISMATCH":
                    return
                delay = 30 if exc.code == "AUTH_REQUIRED" else min(delay * 2, 60)

    async def start(self):
        self._stopping = False
        cancelled = False
        try:
            await self._bounded_connect()
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if not cancelled and not self.interactive and self._last_error != "ACCOUNT_MISMATCH":
                if self._monitor is None or self._monitor.done():
                    self._monitor = asyncio.create_task(self._watch_connection())

    async def close(self):
        self._stopping = True
        monitor, self._monitor = self._monitor, None
        if monitor is not None and monitor is not asyncio.current_task():
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
        self._ready = False
        if self.client is not None:
            await self.client.close()

    def status(self):
        connected = bool(self._ready and self.client and self.client._app.connection.is_open)
        reason = None if connected else self._last_error or "UPSTREAM_ERROR"
        return {"connected": connected, "account_id": self.account_id if connected else None, "reason": reason}

    def _app(self):
        if not self.status()["connected"]:
            code = self.status()["reason"] or "UPSTREAM_ERROR"
            raise BridgeError(code, "Сеанс MAX не подключён", retryable=code == "UPSTREAM_ERROR")
        return self.client._app

    # Для сообщений используем исходные поля, чтобы неизвестные вложения не ломали чтение.
    async def list_chats(self, marker):
        # Возвращаем маркер сервера: обычный метод библиотеки его отбрасывает.
        from pymax.protocol import Opcode

        response = await self._app().invoke(Opcode.CHATS_LIST, {"marker": marker})
        payload = response.payload
        rows = payload.get("chats")
        if not isinstance(rows, list):
            raise BridgeError("UPSTREAM_ERROR", "MAX вернул некорректный список чатов", retryable=True)
        return {"chats": await self._name_dialogs(rows), "marker": payload.get("marker")}

    async def get_chat(self, chat_id):
        try:
            row = await self._app().api.chats.get_chat(int(chat_id))
            return (await self._name_dialogs([row]))[0]
        except BridgeError:
            raise
        except Exception as exc:
            raise BridgeError("CHAT_UNAVAILABLE", "Не удалось получить чат MAX") from exc


    async def _name_dialogs(self, rows):
        # Имя назначается только диалогу с однозначно подтверждённым собеседником.
        chats = [row.model_dump(by_alias=True) if hasattr(row, "model_dump") else dict(row) for row in rows]
        peers = {}
        owner = str(self.account_id)
        for index, chat in enumerate(chats):
            typ = getattr(chat.get("type"), "value", chat.get("type"))
            participants = chat.get("participants") or {}
            if typ != "DIALOG" or chat.get("title") or not isinstance(participants, dict):
                continue
            ids = {str(user_id) for user_id in participants}
            others = ids - {owner}
            if owner in ids and len(others) == 1 and len(ids) == 2:
                try:
                    peers[index] = int(others.pop())
                except ValueError:
                    continue

        if not peers:
            return chats

        # Пользовательские профили получаем одним запросом через существующий сеанс.
        try:
            users = await self._app().api.users.get_users(list(set(peers.values())))
        except Exception as exc:
            raise BridgeError("UPSTREAM_ERROR", "Не удалось получить имена собеседников MAX", retryable=True) from exc
        names = {}
        for user in users:
            for name in user.names:
                full_name = name.name or " ".join(filter(None, (name.first_name, name.last_name)))
                if full_name and full_name.strip():
                    names[user.id] = full_name.strip()
                    break
        for index, peer_id in peers.items():
            if peer_id in names:
                chats[index]["title"] = names[peer_id]
        return chats

    @staticmethod
    def _check_message_identities(rows, chat_id, message_ids):
        # Проверяем только внешние сообщения: вложенная пересылка может быть из другого чата.
        requested = {str(item) for item in message_ids}
        seen = set()
        for row in rows:
            if not isinstance(row, dict):
                raise BridgeError("UPSTREAM_ERROR", "MAX вернул некорректный ответ сообщений")
            ident = row.get("id")
            if (
                type(ident) not in (int, str) or str(ident) not in requested
                or str(ident) in seen
                or any(
                    type(row[key]) not in (int, str) or str(row[key]) != str(chat_id)
                    for key in ("chatId", "chat_id") if key in row
                )
            ):
                raise BridgeError("UPSTREAM_ERROR", "MAX вернул сообщение, не соответствующее запросу")
            seen.add(str(ident))

    async def get_messages(self, chat_id, message_ids):
        from pymax.api.messages.payloads import GetMessagesPayload
        from pymax.protocol import Opcode
        request = GetMessagesPayload(chat_id=int(chat_id), message_ids=[int(x) for x in message_ids])
        frame = await self._app().invoke(Opcode.MSG_GET, request.to_payload())
        payload = frame.payload
        if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
            raise BridgeError("UPSTREAM_ERROR", "MAX вернул некорректный ответ сообщений")
        self._check_message_identities(payload["messages"], request.chat_id, request.message_ids)
        return payload["messages"]

    async def get_history(self, chat_id, *, from_ms, backward=50, forward=0):
        from pymax.api.messages.payloads import ChatHistoryPayload
        from pymax.protocol import Opcode
        request = ChatHistoryPayload(
            chat_id=int(chat_id), forward=forward, backward=backward,
            from_=int(from_ms), interactive=False,
        )
        frame = await self._app().invoke(Opcode.CHAT_HISTORY, request.to_payload())
        payload = frame.payload
        if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
            raise BridgeError("UPSTREAM_ERROR", "MAX вернул некорректный ответ истории")
        return payload["messages"]

    # Сохраняем точный текст и идентификатор без автоматического повтора.
    async def submit(self, payload, cid, uploaded_attachments):
        from pymax.protocol import Opcode
        message = {"text": payload.get("text"), "cid": int(cid), "elements": [], "attaches": uploaded_attachments}
        if payload.get("reply_to") is not None:
            message["link"] = {"type": "REPLY", "messageId": int(payload["reply_to"])}
        frame = await self._app().invoke(
            Opcode.MSG_SEND, {"chatId": int(payload["chat_id"]), "message": message, "notify": True},
        )
        response = frame.payload
        if not isinstance(response, dict) or response.get("id") is None:
            raise BridgeError("OUTCOME_UNKNOWN", "MAX не вернул подтверждённую квитанцию")
        return {"chat_id": str(payload["chat_id"]), "message_id": str(response["id"]), "cid": str(cid)}

    # Адрес вложения предназначен только внутреннему файловому модулю.
    async def resolve_attachment(self, chat_id, message_id, attachment_id):
        rows = await self.get_messages(chat_id, [message_id])
        if len(rows) != 1:
            raise BridgeError("MESSAGE_NOT_FOUND", "Сообщение MAX не найдено")
        # Проверяем внешнюю запись и при подмене метода получения сообщений.
        self._check_message_identities(rows, int(chat_id), [int(message_id)])

        # Проверяем идентификатор по повторно полученному исходному сообщению.
        source_message = rows[0]
        attachment_id = str(attachment_id)
        depth = 0
        # Путь пересылки проверяется заново по сообщению из порученного чата.
        while attachment_id.startswith("forward:"):
            depth += 1
            link = source_message.get("link") or {}
            if depth > 4 or link.get("type") != "FORWARD" or not isinstance(link.get("message"), dict):
                raise BridgeError("ATTACHMENT_CHANGED", "Пересланное вложение отсутствует или изменилось")
            source_message = link["message"]
            attachment_id = attachment_id[len("forward:"):]
        attaches = source_message.get("attaches") or []
        id_fields = {"FILE": "fileId", "PHOTO": "photoId", "VIDEO": "videoId", "AUDIO": "audioId"}
        matches = []
        for index, item in enumerate(attaches):
            field = id_fields.get(str(item.get("_type", "")).upper())
            ident = item.get(field) if field else None
            if str(index if ident is None else ident) == str(attachment_id):
                matches.append(item)
        if len(matches) != 1:
            raise BridgeError("ATTACHMENT_CHANGED", "Вложение отсутствует или изменилось")

        # Документ и видео требуют отдельного запроса; адрес аудио приходит в сообщении.
        item = matches[0]
        kind = str(item.get("_type", "")).upper()
        service = self._app().api.messages
        if kind == "FILE":
            result = await service.get_file_by_id(int(chat_id), int(message_id), int(item["fileId"]))
            url, unsafe = (result.url, result.unsafe) if result else (None, None)
        elif kind == "VIDEO":
            result = await service.get_video_by_id(int(chat_id), int(message_id), int(item["videoId"]))
            url, unsafe = (result.url, None) if result else (None, None)
        elif kind == "PHOTO":
            url, unsafe = item.get("baseUrl"), None
        elif kind == "AUDIO":
            url, unsafe = item.get("url"), None
        else:
            raise BridgeError("MEDIA_UNSUPPORTED", "Этот вид вложения не поддерживается")
        if not url:
            raise BridgeError("MEDIA_UNSUPPORTED", "MAX не предоставил адрес вложения")

        names = {"PHOTO": "photo.jpg", "VIDEO": "video.mp4", "AUDIO": "audio", "FILE": "file"}
        return {
            "url": url, "filename": item.get("name") or names[kind],
            "kind": kind.lower(), "size": item.get("size"), "unsafe": unsafe,
        }

    # Резервируем адрес и подписываемся на сигнал до загрузки.
    async def prepare_upload(self, asset):
        import uuid
        from pymax.api.uploads.payloads import UploadPayload
        from pymax.protocol import Opcode

        kind = asset.get("kind")
        if kind not in ("document", "photo"):
            raise BridgeError("MEDIA_UNSUPPORTED", "Этот вид вложения нельзя отправить")
        frame = await self._app().invoke(
            Opcode.FILE_UPLOAD if kind == "document" else Opcode.PHOTO_UPLOAD,
            UploadPayload().to_payload(),
        )
        data = frame.payload or {}
        upload_id = uuid.uuid4().hex
        if kind == "document":
            try:
                info = data["info"][0]
                file_id, url = int(info["fileId"]), info["url"]
            except (KeyError, IndexError, TypeError, ValueError) as exc:
                raise BridgeError("UPSTREAM_ERROR", "MAX не предоставил данные загрузки файла") from exc
            future = asyncio.get_running_loop().create_future()
            waiters = self._app().api.uploads.file_upload_waiters
            waiters[file_id] = future
            self._uploads[upload_id] = {"kind": kind, "file_id": file_id, "future": future, "waiters": waiters}
            size = int(asset["size"])
            name = quote(str(asset.get("filename") or "file"))
            headers = {
                "Content-Disposition": f"attachment; filename={name}",
                "Content-Length": str(size),
                "Content-Range": f"0-{size - 1}/{size}",
            }
            return {"upload_id": upload_id, "url": url, "method": "POST", "headers": headers}
        try:
            url = data["url"]
        except (KeyError, TypeError) as exc:
            raise BridgeError("UPSTREAM_ERROR", "MAX не предоставил адрес загрузки фотографии") from exc
        query = parse_qs(urlsplit(url).query)
        photo_ids = query.get("photos") or query.get("photoIds") or []
        self._uploads[upload_id] = {"kind": kind, "photo_id": photo_ids[0] if photo_ids else None}
        return {"upload_id": upload_id, "url": url, "method": "POST", "headers": {}, "form_field": "file"}

    async def cancel_upload(self, prepared):
        entry = self._uploads.pop(prepared.get("upload_id"), None)
        if entry and entry["kind"] == "document":
            entry["waiters"].pop(entry["file_id"], None)
            entry["future"].cancel()

    # Ожидаем подтверждение обработки файла после HTTP-загрузки.
    async def finish_upload(self, prepared, response, status, headers=None):
        entry = self._uploads.get(prepared.get("upload_id"))
        if entry is None:
            raise BridgeError("UPSTREAM_ERROR", "Подготовка загрузки не найдена")
        try:
            if status != 200:
                raise BridgeError("UPSTREAM_ERROR", "MAX отклонил загрузку вложения")
            if entry["kind"] == "document":
                try:
                    await asyncio.wait_for(entry["future"], timeout=60)
                except TimeoutError as exc:
                    raise BridgeError("UPSTREAM_ERROR", "MAX не подтвердил обработку файла") from exc
                return {"_type": "FILE", "fileId": entry["file_id"]}
            try:
                photos = json.loads(response)["photos"]
                photo_id = entry["photo_id"] or (next(iter(photos)) if len(photos) == 1 else None)
                token = photos[photo_id]["token"]
                if not isinstance(token, str) or not token:
                    raise ValueError()
            except (ValueError, KeyError, TypeError, StopIteration) as exc:
                raise BridgeError("UPSTREAM_ERROR", "MAX не подтвердил загрузку фотографии") from exc
            return {"_type": "PHOTO", "photoToken": token}
        finally:
            await self.cancel_upload(prepared)
