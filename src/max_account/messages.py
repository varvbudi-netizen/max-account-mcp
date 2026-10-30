from __future__ import annotations

import base64
import hashlib
import json
import time
from datetime import datetime, timezone

from .errors import BridgeError


def _value(item, *names, default=None):
    for name in names:
        if isinstance(item, dict) and name in item:
            return item[name]
        if not isinstance(item, dict) and hasattr(item, name):
            return getattr(item, name)
    return default


def _id(value):
    if isinstance(value, bool) or not str(value).lstrip("-").isdigit():
        raise BridgeError("CURSOR_INVALID", "Некорректный идентификатор MAX")
    return str(value)


def _millis(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        date = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if date.tzinfo is None:
            raise ValueError("timezone required")
        return int(date.timestamp() * 1000)
    except (ValueError, TypeError, OverflowError) as exc:
        raise BridgeError("CURSOR_INVALID", "Укажите дату с часовым поясом") from exc


def _utc(value):
    if isinstance(value, datetime):
        date = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    else:
        number = int(value)
        date = datetime.fromtimestamp(number / 1000, timezone.utc)
    return date.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _cursor_encode(data):
    return base64.urlsafe_b64encode(json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode()).decode().rstrip("=")


def _cursor_decode(cursor):
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        if not isinstance(data, dict):
            raise ValueError()
        return data
    except (ValueError, TypeError, UnicodeError) as exc:
        raise BridgeError("CURSOR_INVALID", "Курсор повреждён") from exc


# Наружу выводим только известные безопасные поля вложений.
def normalize_attachment(item, index):
    kind = str(_value(item, "_type", "type", default="")).upper()
    kinds = {
        "PHOTO": ("photo", "photoId", "photo_id"),
        "FILE": ("document", "fileId", "file_id"),
        "VIDEO": ("video", "videoId", "video_id"),
        "AUDIO": ("audio", "audioId", "audio_id"),
    }
    if kind not in kinds:
        return {"attachment_id": str(index), "kind": "unknown", "downloadable": False}

    # У аудио идентификатор и адрес необязательны; адрес остаётся только внутри службы.
    label, camel, snake = kinds[kind]
    ident = _value(item, camel, snake)
    ident = index if ident is None else ident
    name = _value(item, "name")
    if name:
        name = str(name).replace("\\", "/").split("/")[-1]
    downloadable = bool(_value(item, "url")) if kind == "AUDIO" else True
    return {
        "attachment_id": str(ident), "kind": label, "filename": name,
        "size": _value(item, "size"), "downloadable": downloadable,
    }


def normalize_message(item, chat_id, _depth=0):
    sender = _value(item, "sender")
    sender_id = _value(sender, "id") if not isinstance(sender, (int, str)) else sender
    names = _value(sender, "names", default=[]) or []
    display = _value(sender, "display_name", "displayName")
    if not display and names:
        display = _value(names[0], "name")

    # Пересланное сообщение не выдаём за исходную реплику ответа.
    link = _value(item, "link")
    link_type = _value(link, "type") if link else None
    link_type = getattr(link_type, "value", link_type)
    reply_message = _value(link, "message") if link_type == "REPLY" else None
    reply_id = _value(reply_message, "id") if reply_message else (
        _value(link, "messageId", "message_id") if link_type == "REPLY" else None
    )
    timestamp = _value(item, "time")
    ident = _value(item, "id")
    if _depth == 0 and (timestamp is None or ident is None):
        raise BridgeError("UPSTREAM_ERROR", "MAX вернул сообщение без идентификатора или времени")
    attachments = _value(item, "attaches", default=[]) or []
    result = {
        "chat_id": str(_value(item, "chatId", "chat_id", default=chat_id) or chat_id),
        "message_id": str(ident) if ident is not None else None,
        "sender": {"id": str(sender_id) if sender_id is not None else None, "display_name": display, "is_bot": None},
        "time_utc": _utc(timestamp) if timestamp is not None else None, "text": _value(item, "text", default="") or "",
        "reply_to": str(reply_id) if reply_id is not None else None,
        "attachments": [normalize_attachment(attachment, index) for index, attachment in enumerate(attachments)],
        "link": None,
    }

    # Пересылка содержит отдельного автора и текст: не приписываем их переславшему.
    if link_type == "FORWARD":
        source = _value(link, "message")
        source_chat = _value(link, "chatId", "chat_id")
        source_chat = str(source_chat) if source_chat is not None and str(source_chat).lstrip("-").isdigit() else None
        if source is None:
            result["forwarded"] = {"status": "unavailable"}
        elif _depth >= 4:
            result["forwarded"] = {"status": "depth_limit"}
        else:
            forwarded = normalize_message(source, source_chat or chat_id, _depth + 1)
            if source_chat is None and _value(source, "chatId", "chat_id") is None:
                forwarded["chat_id"] = None
            result["forwarded"] = {"status": "available", "source_chat_id": source_chat, "message": forwarded}
            # Идентификатор хранит путь внутри уже полученной пересылки, без чтения другого чата.
            result["attachments"].extend(
                {**attachment, "attachment_id": "forward:" + attachment["attachment_id"], "source": "forwarded"}
                for attachment in forwarded["attachments"]
            )

    result["content_status"] = (
        "text" if result["text"] else "attachment" if result["attachments"]
        else "forwarded" if result.get("forwarded", {}).get("status") == "available"
        else "unavailable" if "forwarded" in result else "empty"
    )
    return result


def normalize_chat(chat):
    typ = _value(chat, "type")
    if hasattr(typ, "value"):
        typ = typ.value
    return {"chat_id": str(_value(chat, "id")), "title": _value(chat, "title", "name"), "type": str(typ) if typ is not None else None}


class Messages:
    def __init__(self, account):
        self.account = account

    async def list_chats(self, query=None, limit=50, cursor=None):
        if not 1 <= limit <= 200:
            raise BridgeError("CURSOR_INVALID", "Лимит должен быть от 1 до 200")

        # Курсор хранит остаток страницы и уже выданные чаты на временной границе.
        marker = int(time.time() * 1000)
        offset = 0
        fingerprint = None
        uncertain = False
        boundary_time = None
        boundary_ids = []
        if cursor:
            data = _cursor_decode(cursor)
            marker, offset = data.get("marker"), data.get("offset")
            boundary_time, boundary_ids = data.get("boundary_time"), data.get("boundary_ids")
            if (data.get("kind") != "chats" or data.get("query") != query
                    or data.get("account_id") != self.account.account_id
                    or type(marker) is not int or marker <= 0
                    or type(offset) is not int or offset < 0
                    or type(data.get("uncertain")) is not bool
                    or not isinstance(boundary_ids, list)
                    or any(not isinstance(item, str) for item in boundary_ids)
                    or (boundary_time is not None and
                        (type(boundary_time) is not int or marker != boundary_time + 1))
                    or (boundary_time is None and boundary_ids)
                    or (offset > 0 and not isinstance(data.get("fingerprint"), str))):
                raise BridgeError("CURSOR_INVALID", "Курсор не соответствует запросу чатов")
            fingerprint = data.get("fingerprint")
            uncertain = data["uncertain"]

        page = await self.account.list_chats(marker)
        chats = page["chats"]
        rows = [normalize_chat(chat) for chat in chats]

        # Отпечаток выявляет изменения страницы и имён между запросами остатка.
        page_state = [
            (row, _value(chat, "lastEventTime", "last_event_time"))
            for chat, row in zip(chats, rows)
        ]
        page_fingerprint = hashlib.sha256(json.dumps(page_state, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
        if fingerprint is not None and fingerprint != page_fingerprint:
            raise BridgeError("CURSOR_INVALID", "Страница чатов изменилась, начните поиск заново")

        # Перекрываем границу на миллисекунду и убираем только ранее выданные чаты.
        rows = [
            row for chat, row in zip(chats, rows)
            if not (boundary_time is not None
                    and _value(chat, "lastEventTime", "last_event_time") == boundary_time
                    and row["chat_id"] in boundary_ids)
        ]
        unnamed_dialogs = sum(1 for row in rows if row["type"] == "DIALOG" and not row["title"])
        uncertain = uncertain or bool(query and unnamed_dialogs)
        if query:
            rows = [row for row in rows if query.casefold() in (row["title"] or "").casefold()]
        if offset > len(rows):
            raise BridgeError("CURSOR_INVALID", "Страница чатов изменилась, начните поиск заново")

        result = rows[offset:offset + limit]
        next_offset = offset + len(result)
        if next_offset < len(rows):
            next_marker = marker
            fingerprint = page_fingerprint
        else:
            # Нулевой маркер завершает обход; повтор границы означает остановку.
            next_boundary = page["marker"]
            if type(next_boundary) is int and next_boundary == 0:
                return {"chats": result, "partial": uncertain, "end_reached": not uncertain, "next_cursor": None, "reason": "UNVERIFIED" if uncertain else None, "unnamed_dialogs": unnamed_dialogs}
            boundary_rows = [
                row["chat_id"] for chat, row in zip(chats, [normalize_chat(chat) for chat in chats])
                if _value(chat, "lastEventTime", "last_event_time") == next_boundary
            ]
            if (type(next_boundary) is not int or next_boundary <= 0
                    or next_boundary >= marker
                    or (boundary_time is not None and next_boundary >= boundary_time)
                    or not boundary_rows):
                return {"chats": result, "partial": True, "end_reached": False, "next_cursor": None, "reason": "PAGINATION_STALLED", "unnamed_dialogs": unnamed_dialogs}
            next_marker = next_boundary + 1
            boundary_time = next_boundary
            boundary_ids = boundary_rows
            next_offset = 0
            fingerprint = None

        next_cursor = _cursor_encode({"kind": "chats", "account_id": self.account.account_id, "query": query, "marker": next_marker, "offset": next_offset, "fingerprint": fingerprint, "uncertain": uncertain, "boundary_time": boundary_time, "boundary_ids": boundary_ids})
        return {"chats": result, "partial": True, "end_reached": False, "next_cursor": next_cursor, "reason": None, "unnamed_dialogs": unnamed_dialogs}

    async def get_chat(self, chat_id):
        return normalize_chat(await self.account.get_chat(_id(chat_id)))

    # Курсор фиксирует аккаунт, диапазон и уже увиденные сообщения на границе.
    async def get_messages(self, chat_id, limit=50, cursor=None, since=None, until=None):
        chat_id = _id(chat_id)
        if not 1 <= limit <= 200:
            raise BridgeError("CURSOR_INVALID", "Лимит должен быть от 1 до 200")
        account_id = self.account.account_id
        since_ms, until_ms = _millis(since), _millis(until)
        if cursor:
            state = _cursor_decode(cursor)
            if (
                state.get("account") != account_id or state.get("chat") != chat_id
                or state.get("kind") != "history" or not isinstance(state.get("until"), int)
                or not isinstance(state.get("from"), int) or not isinstance(state.get("seen"), list)
                or any(not isinstance(item, str) for item in state.get("seen", []))
                or state.get("since") is not None and not isinstance(state.get("since"), int)
            ):
                raise BridgeError("CURSOR_INVALID", "Курсор относится к другому диапазону или повреждён")
            if (since is not None and since_ms != state.get("since")) or (until is not None and until_ms != state.get("until")):
                raise BridgeError("CURSOR_INVALID", "Фильтры не совпадают с курсором")
            since_ms, until_ms = state.get("since"), state["until"]
        else:
            until_ms = until_ms if until_ms is not None else int(time.time() * 1000)
            state = {"kind": "history", "account": account_id, "chat": chat_id, "since": since_ms, "until": until_ms, "from": until_ms, "seen": []}
        if since_ms is not None and since_ms > until_ms:
            raise BridgeError("CURSOR_INVALID", "Начало диапазона позже его конца")
        # Граничное время включается в ответ, поэтому запрашиваем место для уже выданных записей.
        seen = set(state.get("seen", []))
        window = min(200, limit + len(seen))
        raw = await self.account.get_history(chat_id, from_ms=state["from"], backward=window)
        normalized = [normalize_message(row, chat_id) for row in raw]
        normalized.sort(key=lambda row: (_millis(row["time_utc"]), row["message_id"]), reverse=True)
        page = [row for row in normalized if row["message_id"] not in seen and (since_ms is None or _millis(row["time_utc"]) >= since_ms) and _millis(row["time_utc"]) <= until_ms][:limit]
        if not raw:
            return {"messages": [], "partial": False, "end_reached": True, "next_cursor": None, "since": since_ms, "until": until_ms}
        if not page:
            # Уже выданные записи на границе не мешают распознать выход из диапазона.
            unseen = [row for row in normalized if row["message_id"] not in seen]
            if since_ms is not None and unseen and all(_millis(row["time_utc"]) < since_ms for row in unseen):
                return {"messages": [], "partial": False, "end_reached": True, "next_cursor": None, "since": since_ms, "until": until_ms}
            # Неполный ответ позволил осмотреть всю границу; проверяем историю строго до неё.
            if len(raw) < window and seen and all(_millis(row["time_utc"]) == state["from"] for row in normalized):
                older = await self.account.get_history(chat_id, from_ms=state["from"] - 1, backward=1)
                if not older:
                    return {"messages": [], "partial": False, "end_reached": True, "next_cursor": None, "since": since_ms, "until": until_ms}
            return {"messages": [], "partial": True, "end_reached": False, "next_cursor": None, "reason": "PAGINATION_STALLED", "since": since_ms, "until": until_ms}
        boundary = _millis(page[-1]["time_utc"])
        boundary_seen = [row["message_id"] for row in page if _millis(row["time_utc"]) == boundary]
        if boundary == state["from"]:
            boundary_seen = list(seen | set(boundary_seen))
        next_state = {**state, "from": boundary, "seen": boundary_seen}
        return {"messages": page, "partial": False, "end_reached": False, "next_cursor": _cursor_encode(next_state), "since": since_ms, "until": until_ms}

    async def search_messages(self, chat_id, query, scan_limit=1000, cursor=None, since=None, until=None):
        if not query or not 1 <= scan_limit <= 5000:
            raise BridgeError("CURSOR_INVALID", "Укажите запрос и допустимый предел поиска")
        if cursor:
            saved = _cursor_decode(cursor)
            if saved.get("kind") != "search" or saved.get("query") != query or saved.get("chat") != str(chat_id) or saved.get("account") != self.account.account_id:
                raise BridgeError("CURSOR_INVALID", "Курсор относится к другому поиску")
            current = saved.get("history")
        else:
            current = None
        matches, scanned = [], 0
        last = None
        while scanned < scan_limit:
            last = await self.get_messages(chat_id, limit=min(200, scan_limit - scanned), cursor=current, since=since, until=until)
            page = last["messages"]
            scanned += len(page)
            # Ищем и в исходном содержимом пересылок, не приписывая его автору обёртки.
            for row in page:
                current_message = row
                for _ in range(5):
                    if query.casefold() in current_message.get("text", "").casefold():
                        matches.append(row)
                        break
                    forwarded = current_message.get("forwarded")
                    current_message = forwarded.get("message") if isinstance(forwarded, dict) else None
                    if not isinstance(current_message, dict):
                        break
            current = last["next_cursor"]
            if not current:
                break
        continuation = _cursor_encode({"kind": "search", "query": query, "chat": str(chat_id), "account": self.account.account_id, "history": current}) if current else None
        return {"messages": matches, "scanned": scanned, "next_cursor": continuation, "end_reached": bool(last and last["end_reached"]), "partial": bool(last and (last["partial"] or current)), "reason": last.get("reason") if last else None}

    async def get_context(self, chat_id, message_id, before=10, after=10):
        if not 0 <= before <= 50 or not 0 <= after <= 50:
            raise BridgeError("CURSOR_INVALID", "Число соседних сообщений должно быть от 0 до 50")
        chat_id = _id(chat_id)
        rows = await self.account.get_messages(chat_id, [_id(message_id)])
        if not rows:
            raise BridgeError("MESSAGE_NOT_FOUND", "Сообщение не найдено")
        raw = rows[0]
        target = normalize_message(raw, chat_id)
        neighbours = await self.account.get_history(
            chat_id, from_ms=_millis(target["time_utc"]), backward=before, forward=after,
        )

        # Исходная реплика доступна внутри ссылки; чужие чаты автоматически не открываем.
        reply_source = None
        if target["reply_to"] is not None:
            link = _value(raw, "link")
            source_chat = _value(link, "chatId", "chat_id")
            source = _value(link, "message")
            if source_chat is None:
                reply_source = {"status": "unavailable", "reason": "CHAT_UNCONFIRMED"}
            elif str(source_chat) != chat_id:
                reply_source = {"status": "unavailable", "reason": "CROSS_CHAT"}
            elif source is not None and _value(source, "id") is not None and _value(source, "time") is not None:
                reply_source = {"status": "available", "message": normalize_message(source, chat_id)}
            else:
                originals = await self.account.get_messages(chat_id, [target["reply_to"]])
                if originals:
                    reply_source = {"status": "available", "message": normalize_message(originals[0], chat_id)}
                else:
                    reply_source = {"status": "unavailable", "reason": "MESSAGE_NOT_FOUND"}

        return {
            "message": target,
            "messages": [normalize_message(row, chat_id) for row in neighbours],
            "reply_source": reply_source,
            "partial": True,
        }

    async def get_pinned_messages(self, chat_id, limit=50, cursor=None):
        if cursor or not 1 <= limit <= 200:
            raise BridgeError("CURSOR_INVALID", "Некорректные параметры закреплённых сообщений")
        chat = await self.account.get_chat(_id(chat_id))
        pinned = _value(chat, "pinnedMessage", "pinned_message")
        rows = [normalize_message(pinned, chat_id)] if pinned else []
        # Модель SDK содержит один закреп, но не подтверждает полный список.
        return {"messages": rows, "partial": True, "end_reached": False, "next_cursor": None, "reason": "UNVERIFIED"}

    async def get_forum_topics(self, chat_id, limit=50, cursor=None):
        raise BridgeError("CAPABILITY_UNSUPPORTED", "Темы форума в MAX пока не подтверждены")
