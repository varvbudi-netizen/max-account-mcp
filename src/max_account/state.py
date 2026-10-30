"""Устойчивое состояние отправок, файлов и пакетов заданий."""

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from .errors import BridgeError

MAX_BATCH_RESPONSE_BYTES = 64 * 1024 * 1024
BATCH_RESPONSE_RETENTION_SECONDS = 24 * 60 * 60


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _millis(value):
    # Все границы истории в базе выражены целыми миллисекундами Unix.
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        try:
            date = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if date.tzinfo is not None:
                return int(date.timestamp() * 1000)
        except (ValueError, OverflowError):
            pass
    raise BridgeError("CURSOR_INVALID", "Укажите миллисекунды Unix или дату с часовым поясом")


def _validate_request_key(request_key):
    if not isinstance(request_key, str) or not 1 <= len(request_key) <= 160 or not request_key.strip():
        raise BridgeError("REQUEST_CONFLICT", "Некорректный ключ поручения")


class StateStore:
    """Короткие независимые соединения позволяют нескольким клиентам делить базу."""

    def __init__(self, db_path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS operations (
                    operation_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE,
                    payload TEXT NOT NULL, fingerprint TEXT NOT NULL, batch_id TEXT, cid INTEGER UNIQUE,
                    status TEXT NOT NULL, receipt TEXT, error TEXT
                );
                CREATE TABLE IF NOT EXISTS assets (
                    file_id TEXT PRIMARY KEY, request_key TEXT UNIQUE,
                    batch_id TEXT, status TEXT, data TEXT NOT NULL, cleanup_attempt REAL
                );
                CREATE TABLE IF NOT EXISTS operation_assets (
                    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
                    file_id TEXT NOT NULL REFERENCES assets(file_id),
                    PRIMARY KEY(operation_id, file_id)
                );
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY, task_key TEXT NOT NULL,
                    chat_ids TEXT NOT NULL, initial_since TEXT, upper_bound INTEGER NOT NULL,
                    checkpoint_version INTEGER NOT NULL, status TEXT NOT NULL, committed_at REAL
                );
                CREATE TABLE IF NOT EXISTS transfers (
                    transfer_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE,
                    fingerprint TEXT NOT NULL, batch_id TEXT, status TEXT NOT NULL,
                    progress_bytes INTEGER NOT NULL DEFAULT 0, result TEXT, error TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_open_batch ON batches(task_key)
                    WHERE status = 'open';
                CREATE TABLE IF NOT EXISTS batch_pages (
                    batch_id TEXT NOT NULL, chat_id TEXT NOT NULL, cursor TEXT,
                    since_value TEXT,
                    end_reached INTEGER NOT NULL DEFAULT 0, partial INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(batch_id, chat_id)
                );
                CREATE TABLE IF NOT EXISTS batch_results (
                    batch_id TEXT NOT NULL, chat_id TEXT NOT NULL,
                    request_cursor TEXT NOT NULL, response TEXT NOT NULL, created_at REAL NOT NULL,
                    PRIMARY KEY(batch_id, chat_id, request_cursor)
                );
                CREATE TABLE IF NOT EXISTS checkpoints (
                    task_key TEXT NOT NULL, chat_id TEXT NOT NULL, version INTEGER NOT NULL,
                    since INTEGER NOT NULL, PRIMARY KEY(task_key, chat_id)
                );
                CREATE TABLE IF NOT EXISTS sequences (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
                INSERT OR IGNORE INTO sequences VALUES ('cid', 0);
                PRAGMA user_version=1;
            """)
            # Базы, созданные ранней разработческой версией, получают сохранённые границы открытых пакетов.
            db.execute("BEGIN IMMEDIATE")
            columns = {row["name"] for row in db.execute("PRAGMA table_info(batch_pages)")}
            if "since_value" not in columns:
                db.execute("ALTER TABLE batch_pages ADD COLUMN since_value TEXT")
                for row in db.execute("""SELECT p.batch_id, p.chat_id, b.initial_since, c.since
                    FROM batch_pages p JOIN batches b ON b.batch_id=p.batch_id
                    LEFT JOIN checkpoints c ON c.task_key=b.task_key AND c.chat_id=p.chat_id
                    WHERE b.status='open'""").fetchall():
                    since = row["since"] if row["since"] is not None else json.loads(row["initial_since"])
                    db.execute("UPDATE batch_pages SET since_value=? WHERE batch_id=? AND chat_id=?",
                               (_json(since), row["batch_id"], row["chat_id"]))
            asset_columns = {row["name"] for row in db.execute("PRAGMA table_info(assets)")}
            if "cleanup_attempt" not in asset_columns:
                db.execute("ALTER TABLE assets ADD COLUMN cleanup_attempt REAL")
            operation_columns = {row["name"] for row in db.execute("PRAGMA table_info(operations)")}
            if "claim_token" not in operation_columns:
                db.execute("ALTER TABLE operations ADD COLUMN claim_token TEXT")
            batch_columns = {row["name"] for row in db.execute("PRAGMA table_info(batches)")}
            if "committed_at" not in batch_columns:
                db.execute("ALTER TABLE batches ADD COLUMN committed_at REAL")
                db.execute("UPDATE batches SET committed_at=? WHERE status='committed'", (time.time(),))
            result_columns = {row["name"] for row in db.execute("PRAGMA table_info(batch_results)")}
            if "created_at" not in result_columns:
                db.execute("ALTER TABLE batch_results ADD COLUMN created_at REAL")
                db.execute("UPDATE batch_results SET created_at=?", (time.time(),))
            db.execute("""DELETE FROM batch_results WHERE rowid IN
                (SELECT rowid FROM (SELECT rowid, ROW_NUMBER() OVER
                 (PARTITION BY batch_id, chat_id ORDER BY rowid DESC) AS position FROM batch_results)
                 WHERE position>2)""")
            self._prune_batch_results(db)
            db.commit()

    # Завершённые пакеты хранят ответы сутки; открытые пакеты по времени не очищаются.
    @staticmethod
    def _prune_batch_results(db):
        db.execute("""DELETE FROM batch_results WHERE batch_id IN
            (SELECT batch_id FROM batches WHERE status='committed' AND committed_at<=?)""",
            (time.time() - BATCH_RESPONSE_RETENTION_SECONDS,))

    @contextmanager
    def _connect(self, write=False):
        db = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            # SQLite затирает освобождаемые ячейки; журналы и резервные копии имеют отдельный срок жизни.
            db.execute("PRAGMA secure_delete=ON")
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.commit()
        except BaseException:
            if write:
                db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _operation(row):
        if row is None:
            raise BridgeError("NOT_FOUND", "Операция не найдена")
        result = dict(row)
        result.pop("claim_token", None)
        for field in ("payload", "receipt", "error"):
            result[field] = json.loads(result[field]) if result[field] is not None else None
        result["draft_id"] = result["operation_id"]
        return result

    # Один ключ всегда означает одно неизменяемое поручение.
    def prepare(self, request_key, payload, batch_id=None, fingerprint=None):
        _validate_request_key(request_key)
        encoded = _json(payload)
        identity = _json(payload if fingerprint is None else fingerprint)
        with self._connect(write=True) as db:
            row = db.execute("SELECT * FROM operations WHERE request_key=?", (request_key,)).fetchone()
            if row:
                if row["fingerprint"] != identity or row["batch_id"] != batch_id:
                    raise BridgeError("REQUEST_CONFLICT", "Ключ уже связан с другим поручением")
                return self._operation(row)
            if batch_id and not db.execute(
                "SELECT 1 FROM batches WHERE batch_id=? AND status='open'", (batch_id,)
            ).fetchone():
                raise BridgeError("CHECKPOINT_CONFLICT", "Пакет отсутствует или уже завершён")
            # Проверка файлов и их привязка выполняются вместе с созданием черновика.
            file_ids = {asset["file_id"] for asset in payload.get("attachments", [])}
            for file_id in file_ids:
                if not db.execute("SELECT 1 FROM assets WHERE file_id=? AND status IN ('ready','preserved')", (file_id,)).fetchone():
                    raise BridgeError("FILE_EXPIRED", "Снимок файла больше недоступен")
            operation_id = uuid.uuid4().hex
            db.execute(
                "INSERT INTO operations(operation_id, request_key, payload, fingerprint, batch_id, status) VALUES (?, ?, ?, ?, ?, 'prepared')",
                (operation_id, request_key, encoded, identity, batch_id),
            )
            db.executemany("INSERT INTO operation_assets(operation_id,file_id) VALUES (?,?)",
                           [(operation_id, file_id) for file_id in file_ids])
            return self._operation(db.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone())

    def get_operation(self, operation_id=None, request_key=None):
        if (operation_id is None) == (request_key is None):
            raise ValueError("Укажите один идентификатор операции")
        with self._connect() as db:
            field, value = ("operation_id", operation_id) if operation_id is not None else ("request_key", request_key)
            return self._operation(db.execute(f"SELECT * FROM operations WHERE {field}=?", (value,)).fetchone())

    # Захват разрешён лишь до сетевой границы; неизвестный исход не повторяется.
    def claim(self, operation_id, claim_token=None):
        with self._connect(write=True) as db:
            operation = db.execute("SELECT status, batch_id, payload FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            if not operation or operation["status"] not in ('prepared', 'failed_before_send'):
                return False
            if operation["batch_id"] and not db.execute(
                "SELECT 1 FROM batches WHERE batch_id=? AND status='open'", (operation["batch_id"],)
            ).fetchone():
                raise BridgeError("CHECKPOINT_CONFLICT", "Пакет операции уже закрыт")
            # Повтор до отправки не должен захватывать уже истёкший или удаляемый снимок.
            for asset in json.loads(operation["payload"]).get("attachments", []):
                if not db.execute("SELECT 1 FROM assets WHERE file_id=? AND status IN ('ready','preserved')", (asset["file_id"],)).fetchone():
                    raise BridgeError("FILE_EXPIRED", "Снимок файла больше недоступен")
            active = db.execute("SELECT operation_id FROM operations WHERE status IN ('preparing','sending') LIMIT 1").fetchone()
            if active:
                raise BridgeError("BUSY", "Другая отправка уже выполняется", retryable=True, retry_after_seconds=1)
            changed = db.execute(
                "UPDATE operations SET status='preparing', error=NULL, claim_token=? WHERE operation_id=? AND status IN ('prepared','failed_before_send')",
                (claim_token or uuid.uuid4().hex, operation_id),
            ).rowcount
            return bool(changed)

    # Возвращаем только собственный захват, для которого отправитель ещё не вызывался.
    def recover_before_send(self, operation_id, claim_token):
        with self._connect(write=True) as db:
            return bool(db.execute("""UPDATE operations SET status='failed_before_send', claim_token=NULL,
                error=? WHERE operation_id=? AND claim_token=? AND status IN ('preparing','sending')""",
                (_json({"code": "STORAGE_ERROR"}), operation_id, claim_token)).rowcount)

    def mark_sending(self, operation_id):
        with self._connect(write=True) as db:
            row = db.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            if not row or row["status"] != "preparing":
                raise BridgeError("REQUEST_CONFLICT", "Операция не захвачена для отправки")
            cid = row["cid"]
            if cid is None:
                db.execute("UPDATE sequences SET value=MAX(value+1, ?) WHERE name='cid'", (time.time_ns() // 1_000_000,))
                cid = db.execute("SELECT value FROM sequences WHERE name='cid'").fetchone()[0]
            db.execute("UPDATE operations SET status='sending', cid=? WHERE operation_id=?", (cid, operation_id))
            return self._operation(db.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone())

    def finish(self, operation_id, status, receipt=None, error=None):
        allowed = {
            "preparing": {"failed_before_send"},
            "sending": {"sent", "rejected", "unknown"},
            "unknown": {"sent", "abandoned", "unknown"},
            "prepared": {"abandoned"},
            "failed_before_send": {"abandoned"},
        }
        with self._connect(write=True) as db:
            row = db.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            if not row:
                return self._operation(row)
            if status not in allowed.get(row["status"], set()):
                raise BridgeError("REQUEST_CONFLICT", "Недопустимый переход состояния отправки")
            if status == "sent" and receipt is None:
                raise BridgeError("REQUEST_CONFLICT", "Для подтверждения отправки нужна квитанция")
            db.execute("UPDATE operations SET status=?, receipt=?, error=? WHERE operation_id=?", (
                status, _json(receipt) if receipt is not None else None,
                _json(error) if error is not None else None, operation_id,
            ))
            return self._operation(db.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone())

    def recover_interrupted(self):
        with self._connect(write=True) as db:
            db.execute("UPDATE operations SET status='unknown' WHERE status='sending'")
            db.execute("UPDATE operations SET status='failed_before_send' WHERE status='preparing'")
            db.execute("UPDATE transfers SET status='failed', error=? WHERE status IN ('pending','downloading')",
                       (_json({"code": "UPSTREAM_ERROR", "message": "Передача прервана"}),))

    # В пакете фиксируются верхняя граница, набор чатов и исходная версия.
    def begin_batch(self, task_key, chat_ids, initial_since=None):
        chat_ids = list(dict.fromkeys(str(chat_id) for chat_id in chat_ids))
        initial_since = _millis(initial_since)
        if not task_key or not chat_ids:
            raise BridgeError("CHECKPOINT_CONFLICT", "Заданию нужны ключ и хотя бы один чат")
        with self._connect(write=True) as db:
            self._prune_batch_results(db)
            row = db.execute("SELECT * FROM batches WHERE task_key=? AND status='open'", (task_key,)).fetchone()
            if row:
                return self._batch(db, row)
            version = db.execute("SELECT COALESCE(MAX(version),0) FROM checkpoints WHERE task_key=?", (task_key,)).fetchone()[0]
            batch_id = uuid.uuid4().hex
            db.execute("INSERT INTO batches(batch_id,task_key,chat_ids,initial_since,upper_bound,checkpoint_version,status) VALUES (?,?,?,?,?,?, 'open')", (
                batch_id, task_key, _json(chat_ids), _json(initial_since), time.time_ns() // 1_000_000, version,
            ))
            for chat_id in chat_ids:
                checkpoint = db.execute("SELECT since FROM checkpoints WHERE task_key=? AND chat_id=?", (task_key, chat_id)).fetchone()
                since = checkpoint["since"] if checkpoint else initial_since
                db.execute("INSERT INTO batch_pages(batch_id, chat_id, since_value) VALUES (?,?,?)",
                           (batch_id, chat_id, _json(since)))
            return self._batch(db, db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone())

    # Чтение пакета делается одним снимком базы, чтобы курсоры и версия не расходились.
    def get_batch(self, batch_id):
        with self._connect() as db:
            db.execute("BEGIN")
            row = db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not row:
                raise BridgeError("CHECKPOINT_CONFLICT", "Пакет не найден")
            return self._batch(db, row)

    def _batch(self, db, row):
        result = dict(row)
        result["chat_ids"] = json.loads(result["chat_ids"])
        result["initial_since"] = json.loads(result["initial_since"])
        result["current_checkpoint_version"] = db.execute(
            "SELECT COALESCE(MAX(version),0) FROM checkpoints WHERE task_key=?", (row["task_key"],)
        ).fetchone()[0]
        result["pages"] = []
        for page in db.execute("SELECT * FROM batch_pages WHERE batch_id=? ORDER BY chat_id", (row["batch_id"],)):
            item = dict(page)
            item["cursor"] = json.loads(item["cursor"]) if item["cursor"] is not None else None
            item["end_reached"] = bool(item["end_reached"])
            item["partial"] = bool(item["partial"])
            item["since"] = json.loads(item.pop("since_value")) if item["since_value"] is not None else None
            result["pages"].append(item)
        return result

    def get_recorded_page(self, batch_id, chat_id, request_cursor):
        with self._connect(write=True) as db:
            self._prune_batch_results(db)
            row = db.execute("SELECT response FROM batch_results WHERE batch_id=? AND chat_id=? AND request_cursor=?",
                             (batch_id, str(chat_id), _json(request_cursor))).fetchone()
            return json.loads(row["response"]) if row else None

    # Продвижение и ответ сохраняются вместе для безопасного повтора после обрыва.
    def record_page(self, batch_id, chat_id, cursor, end_reached, partial, *, expected_cursor, since, until, response):
        with self._connect(write=True) as db:
            self._prune_batch_results(db)
            saved = db.execute("SELECT response FROM batch_results WHERE batch_id=? AND chat_id=? AND request_cursor=?",
                               (batch_id, str(chat_id), _json(expected_cursor))).fetchone()
            if saved:
                return json.loads(saved["response"])
            batch = db.execute("SELECT * FROM batches WHERE batch_id=? AND status='open'", (batch_id,)).fetchone()
            page = db.execute("SELECT * FROM batch_pages WHERE batch_id=? AND chat_id=?", (batch_id, str(chat_id))).fetchone()
            if not batch or not page:
                raise BridgeError("CHECKPOINT_CONFLICT", "Чат не входит в открытый пакет")
            expected_since = json.loads(page["since_value"]) if page["since_value"] is not None else None
            old_cursor = json.loads(page["cursor"]) if page["cursor"] is not None else None
            if page["end_reached"] and not page["partial"]:
                raise BridgeError("CHECKPOINT_CONFLICT", "Диапазон чата уже завершён")
            if expected_cursor != old_cursor or since != expected_since or until != batch["upper_bound"]:
                raise BridgeError("CHECKPOINT_CONFLICT", "Курсор или границы страницы не совпадают с пакетом")
            if not isinstance(response, dict) or any(response.get(key) != value for key, value in (
                ("next_cursor", cursor), ("end_reached", bool(end_reached)), ("partial", bool(partial)),
                ("since", since), ("until", until),
            )):
                raise BridgeError("CHECKPOINT_CONFLICT", "Ответ страницы не соответствует её продвижению")
            encoded_response = _json(response)
            total = db.execute("SELECT COALESCE(SUM(LENGTH(CAST(response AS BLOB))),0) FROM batch_results").fetchone()[0]
            removable = db.execute("""SELECT COALESCE(SUM(LENGTH(CAST(response AS BLOB))),0)
                FROM batch_results WHERE batch_id=? AND chat_id=? AND rowid NOT IN
                (SELECT rowid FROM batch_results WHERE batch_id=? AND chat_id=? ORDER BY rowid DESC LIMIT 1)""",
                (batch_id, str(chat_id), batch_id, str(chat_id))).fetchone()[0]
            if total - removable + len(encoded_response.encode("utf-8")) > MAX_BATCH_RESPONSE_BYTES:
                raise BridgeError("RESOURCE_LIMIT", "Хранилище результатов пакетов заполнено")
            changed = db.execute("""UPDATE batch_pages SET cursor=?, end_reached=?, partial=?
                WHERE batch_id=? AND chat_id=? AND EXISTS
                (SELECT 1 FROM batches WHERE batch_id=? AND status='open')""", (
                _json(cursor), bool(end_reached), bool(partial), batch_id, str(chat_id), batch_id,
            )).rowcount
            if not changed:
                raise BridgeError("CHECKPOINT_CONFLICT", "Чат не входит в открытый пакет")
            db.execute("INSERT INTO batch_results(batch_id,chat_id,request_cursor,response,created_at) VALUES (?,?,?,?,?)",
                       (batch_id, str(chat_id), _json(expected_cursor), encoded_response, time.time()))
            # Оставляем два последних ответа; переполнение откатывает и удаление, и продвижение.
            db.execute("""DELETE FROM batch_results WHERE batch_id=? AND chat_id=? AND rowid NOT IN
                (SELECT rowid FROM batch_results WHERE batch_id=? AND chat_id=? ORDER BY rowid DESC LIMIT 2)""",
                (batch_id, str(chat_id), batch_id, str(chat_id)))
            return response

    def commit_batch(self, batch_id, expected_checkpoint_version):
        with self._connect(write=True) as db:
            row = db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not row or row["status"] != "open":
                raise BridgeError("CHECKPOINT_CONFLICT", "Пакет отсутствует или уже завершён")
            version = db.execute("SELECT COALESCE(MAX(version),0) FROM checkpoints WHERE task_key=?", (row["task_key"],)).fetchone()[0]
            if version != expected_checkpoint_version or version != row["checkpoint_version"]:
                raise BridgeError("CHECKPOINT_CONFLICT", "Версия отметки изменилась")
            incomplete = db.execute("SELECT 1 FROM batch_pages WHERE batch_id=? AND (end_reached=0 OR partial=1)", (batch_id,)).fetchone()
            pending = db.execute("SELECT 1 FROM operations WHERE batch_id=? AND status NOT IN ('sent','abandoned')", (batch_id,)).fetchone()
            assets = db.execute("SELECT 1 FROM assets WHERE batch_id=? AND COALESCE(status,'') NOT IN ('ready','completed','abandoned')", (batch_id,)).fetchone()
            transfers = db.execute("SELECT 1 FROM transfers WHERE batch_id=? AND status NOT IN ('ready','completed','abandoned')", (batch_id,)).fetchone()
            if incomplete or pending or assets or transfers:
                raise BridgeError("CHECKPOINT_CONFLICT", "Пакет содержит незавершённые страницы или действия")
            for chat_id in json.loads(row["chat_ids"]):
                db.execute("""INSERT INTO checkpoints(task_key,chat_id,version,since) VALUES (?,?,?,?)
                    ON CONFLICT(task_key,chat_id) DO UPDATE SET version=excluded.version,since=excluded.since""",
                    (row["task_key"], chat_id, version + 1, row["upper_bound"]))
            db.execute("UPDATE batches SET status='committed', committed_at=? WHERE batch_id=?", (time.time(), batch_id))
            return self._batch(db, db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone())

    # Отказ освобождает страницы, но не меняет отметку и журнал действий.
    def abandon_batch(self, batch_id):
        with self._connect(write=True) as db:
            row = db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not row:
                raise BridgeError("CHECKPOINT_CONFLICT", "Пакет не найден")
            if row["status"] == "abandoned":
                return self._batch(db, row)
            if row["status"] != "open":
                raise BridgeError("CHECKPOINT_CONFLICT", "Завершённый пакет нельзя отменить")
            sending = db.execute("SELECT 1 FROM operations WHERE batch_id=? AND status IN ('preparing','sending')", (batch_id,)).fetchone()
            downloading = db.execute("SELECT 1 FROM transfers WHERE batch_id=? AND status IN ('pending','downloading')", (batch_id,)).fetchone()
            if sending or downloading:
                raise BridgeError("BUSY", "Действия пакета ещё выполняются", retryable=True, retry_after_seconds=1)
            db.execute("UPDATE batches SET status='abandoned' WHERE batch_id=?", (batch_id,))
            db.execute("DELETE FROM batch_results WHERE batch_id=?", (batch_id,))
            return self._batch(db, db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone())

    # Метаданные файла сохраняются целиком, чтобы владелец файлового слоя мог их проверять.
    def save_asset(self, asset):
        with self._connect(write=True) as db:
            changed = db.execute("""INSERT INTO assets(file_id,request_key,batch_id,status,data) VALUES (?,?,?,?,?)
                ON CONFLICT(file_id) DO UPDATE SET request_key=excluded.request_key,
                batch_id=excluded.batch_id,status=excluded.status,data=excluded.data
                WHERE COALESCE(assets.status,'')!='deleting'""", (
                asset["file_id"], asset.get("request_key"), asset.get("batch_id"), asset.get("status"), _json(asset),
            )).rowcount
            if not changed:
                raise BridgeError("FILE_EXPIRED", "Файл уже удаляется")
        return asset

    def get_asset(self, file_id=None, request_key=None):
        if (file_id is None) == (request_key is None):
            raise ValueError("Укажите один идентификатор файла")
        with self._connect() as db:
            field, value = ("file_id", file_id) if file_id is not None else ("request_key", request_key)
            row = db.execute(f"SELECT data FROM assets WHERE {field}=? AND status NOT IN ('deleting','staging')", (value,)).fetchone()
            return json.loads(row[0]) if row else None

    # Промежуточная запись позволяет после перезапуска найти файл до его выдачи клиенту.
    def get_staged_assets(self):
        with self._connect() as db:
            return [json.loads(row[0]) for row in db.execute("SELECT data FROM assets WHERE status='staging'")]

    def finish_staged_asset(self, file_id, asset):
        with self._connect(write=True) as db:
            if asset is None:
                return bool(db.execute("DELETE FROM assets WHERE file_id=? AND status='staging'", (file_id,)).rowcount)
            if asset.get("file_id") != file_id or asset.get("status") != "ready":
                raise ValueError("Для завершения нужны метаданные готового файла")
            return bool(db.execute("""UPDATE assets SET request_key=?, batch_id=?, status='ready', data=?
                WHERE file_id=? AND status='staging'""",
                (asset.get("request_key"), asset.get("batch_id"), _json(asset), file_id)).rowcount)

    # После исключительного захвата службы незавершённое удаление можно безопасно повторить.
    def recover_asset_deletions(self):
        with self._connect(write=True) as db:
            db.execute("UPDATE assets SET status='ready' WHERE status='deleting'")

    def _claim_asset_rows(self, db, condition, parameters, limit, *, unclaimed_only):
        linked = "1" if unclaimed_only else "(o.status IN ('prepared','preparing','sending','unknown') OR ob.status='open')"
        rows = db.execute(f"""SELECT a.file_id, a.data FROM assets a
            LEFT JOIN batches ab ON ab.batch_id=a.batch_id
            WHERE a.status='ready' AND (ab.batch_id IS NULL OR ab.status!='open') AND {condition}
            AND NOT EXISTS (
                SELECT 1 FROM operation_assets oa JOIN operations o ON o.operation_id=oa.operation_id
                LEFT JOIN batches ob ON ob.batch_id=o.batch_id WHERE oa.file_id=a.file_id AND {linked}
            ) AND NOT EXISTS (
                SELECT 1 FROM transfers t LEFT JOIN batches tb ON tb.batch_id=t.batch_id
                WHERE json_valid(t.result) AND json_extract(t.result,'$.file_id')=a.file_id
                AND (t.status IN ('pending','downloading') OR tb.status='open')
            ) ORDER BY COALESCE(a.cleanup_attempt, 0), a.file_id LIMIT ?""", (*parameters, limit)).fetchall()
        for row in rows:
            db.execute("UPDATE assets SET status='deleting', cleanup_attempt=? WHERE file_id=?", (time.time(), row["file_id"]))
        return [json.loads(row["data"]) for row in rows]

    # Срок проверяется по времени создания в метаданных, а защита связей — в одной транзакции.
    def claim_expired_assets(self, cutoff, limit=50):
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("Лимит должен быть положительным")
        with self._connect(write=True) as db:
            return self._claim_asset_rows(db,
                "json_type(a.data,'$.created_at') IN ('integer','real') AND json_extract(a.data,'$.created_at')<=?",
                (cutoff,), limit, unclaimed_only=False)

    def claim_unclaimed_assets(self, file_ids):
        file_ids = list(set(file_ids))
        if not file_ids:
            return []
        with self._connect(write=True) as db:
            marks = ",".join("?" for _ in file_ids)
            return self._claim_asset_rows(db, f"a.file_id IN ({marks})", file_ids, len(file_ids), unclaimed_only=True)

    # Метаданные удаляются только после подтверждения физического удаления файла.
    def finish_asset_deletion(self, file_id, *, deleted, preserved=False):
        if deleted and preserved:
            raise ValueError("Нельзя одновременно удалить и сохранить файл")
        with self._connect(write=True) as db:
            row = db.execute("SELECT status FROM assets WHERE file_id=?", (file_id,)).fetchone()
            if not row:
                return False
            if row["status"] != "deleting":
                raise BridgeError("REQUEST_CONFLICT", "Файл не захвачен для удаления")
            if deleted:
                db.execute("DELETE FROM operation_assets WHERE file_id=?", (file_id,))
                db.execute("DELETE FROM assets WHERE file_id=?", (file_id,))
            elif preserved:
                db.execute("UPDATE assets SET status='preserved' WHERE file_id=?", (file_id,))
            else:
                db.execute("UPDATE assets SET status='ready' WHERE file_id=?", (file_id,))
            return True

    # Ключ передачи выделяется до работы и разделяется всеми клиентами.
    @staticmethod
    def _transfer(row):
        if row is None:
            return None
        result = dict(row)
        for name in ("result", "error"):
            result[name] = json.loads(result[name]) if result[name] is not None else None
        return result

    def claim_transfer(self, request_key, fingerprint, batch_id=None):
        _validate_request_key(request_key)
        fingerprint = _json(fingerprint)
        with self._connect(write=True) as db:
            row = db.execute("SELECT * FROM transfers WHERE request_key=?", (request_key,)).fetchone()
            if row:
                if row["fingerprint"] != fingerprint or row["batch_id"] != batch_id:
                    raise BridgeError("REQUEST_CONFLICT", "Ключ уже связан с другой передачей")
                return self._transfer(row), False
            if batch_id and not db.execute("SELECT 1 FROM batches WHERE batch_id=? AND status='open'", (batch_id,)).fetchone():
                raise BridgeError("CHECKPOINT_CONFLICT", "Пакет отсутствует или уже завершён")
            transfer_id = uuid.uuid4().hex
            db.execute("INSERT INTO transfers(transfer_id,request_key,fingerprint,batch_id,status) VALUES (?,?,?,?, 'pending')",
                       (transfer_id, request_key, fingerprint, batch_id))
            return self._transfer(db.execute("SELECT * FROM transfers WHERE transfer_id=?", (transfer_id,)).fetchone()), True

    def get_transfer(self, transfer_id=None, request_key=None):
        if (transfer_id is None) == (request_key is None):
            raise ValueError("Укажите один идентификатор передачи")
        with self._connect() as db:
            field, value = ("transfer_id", transfer_id) if transfer_id is not None else ("request_key", request_key)
            return self._transfer(db.execute(f"SELECT * FROM transfers WHERE {field}=?", (value,)).fetchone())

    def update_transfer(self, transfer_id, fields):
        permitted = {"status", "progress_bytes", "result", "error"}
        if not fields or not set(fields) <= permitted:
            raise ValueError("Недопустимые поля передачи")
        with self._connect(write=True) as db:
            result = fields.get("result")
            if isinstance(result, dict) and result.get("file_id") and not db.execute(
                "SELECT 1 FROM assets WHERE file_id=? AND status='ready'", (result["file_id"],)
            ).fetchone():
                raise BridgeError("FILE_EXPIRED", "Файл передачи больше недоступен")
            values = [(_json(value) if key in {"result", "error"} and value is not None else value) for key, value in fields.items()]
            changed = db.execute("UPDATE transfers SET " + ", ".join(f"{key}=?" for key in fields) + " WHERE transfer_id=?", (*values, transfer_id)).rowcount
            if not changed:
                raise BridgeError("NOT_FOUND", "Передача не найдена")
            return self._transfer(db.execute("SELECT * FROM transfers WHERE transfer_id=?", (transfer_id,)).fetchone())
