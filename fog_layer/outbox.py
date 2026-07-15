"""
outbox.py

The fog node's local buffer. Every processed batch or anomaly alert is
written here FIRST, and only deleted once it has been confirmed
delivered. This is what makes the fog node durable across a
connectivity outage: nothing is lost just because the network is down
-- it just waits here until it can go out.

Thread-safe via a single lock guarding all access (sqlite3 connections
are not safe to share across threads without one).
"""

import json
import sqlite3
import threading
import time


class Outbox:
    def __init__(self, db_path):
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_retry_at REAL NOT NULL DEFAULT 0
            )
        """)
        self._conn.commit()

    def insert(self, payload: dict, priority: int = 0):
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO outbox (payload, created_at, priority, attempts, next_retry_at) "
                "VALUES (?, ?, ?, 0, ?)",
                (json.dumps(payload), now, priority, now),
            )
            self._conn.commit()

    def get_next_ready(self, limit: int = 1):
        """Highest priority first, then oldest first. Only rows whose
        backoff window has elapsed are eligible."""
        now = time.time()
        with self._lock:
            cur = self._conn.execute(
                "SELECT id, payload, priority, attempts FROM outbox "
                "WHERE next_retry_at <= ? "
                "ORDER BY priority DESC, created_at ASC LIMIT ?",
                (now, limit),
            )
            rows = cur.fetchall()
        return [
            {"id": r[0], "payload": json.loads(r[1]), "priority": r[2], "attempts": r[3]}
            for r in rows
        ]

    def mark_sent(self, row_id: int):
        with self._lock:
            self._conn.execute("DELETE FROM outbox WHERE id = ?", (row_id,))
            self._conn.commit()

    def mark_failed(self, row_id: int, attempts: int, backoff_base: float, backoff_max: float):
        new_attempts = attempts + 1
        backoff = min(backoff_base * (2 ** (new_attempts - 1)), backoff_max)
        next_retry_at = time.time() + backoff
        with self._lock:
            self._conn.execute(
                "UPDATE outbox SET attempts = ?, next_retry_at = ? WHERE id = ?",
                (new_attempts, next_retry_at, row_id),
            )
            self._conn.commit()
        return backoff

    def purge_older_than(self, max_age_hours: float):
        """Bounded-buffer enforcement: once data exceeds the retention
        cap, drop the OLDEST entries first (not the newest) -- historical
        continuity is prioritised over only protecting the latest
        reading. Returns the number of rows purged."""
        cutoff = time.time() - (max_age_hours * 3600)
        with self._lock:
            cur = self._conn.execute("DELETE FROM outbox WHERE created_at < ?", (cutoff,))
            self._conn.commit()
            return cur.rowcount

    def counts(self):
        with self._lock:
            cur = self._conn.execute(
                "SELECT priority, COUNT(*) FROM outbox GROUP BY priority"
            )
            rows = cur.fetchall()
        return {priority: count for priority, count in rows}

    def total(self):
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) FROM outbox")
            return cur.fetchone()[0]

    def close(self):
        with self._lock:
            self._conn.close()
