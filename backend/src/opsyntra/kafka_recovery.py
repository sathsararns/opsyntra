"""Durable outbox and delivery ledger; offsets are committed only after this ledger."""
import json
import sqlite3
from pathlib import Path

from .kafka_events import utc_now


class RecoveryStore:
    def __init__(self, path, identity):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=10)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                record_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, topic TEXT NOT NULL,
                message_key TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL,
                created_at TEXT NOT NULL, sent_at TEXT, output_partition INTEGER, output_offset INTEGER
            );
        """)
        expected = json.dumps(identity, sort_keys=True)
        previous = self.connection.execute("SELECT value FROM metadata WHERE key='identity'").fetchone()
        if previous and previous[0] != expected:
            self.connection.close()
            raise ValueError("Recovery store belongs to a different model/group/topic/cluster. Use a new --group-id for a new model.")
        with self.connection:
            self.connection.execute("INSERT OR IGNORE INTO metadata VALUES ('identity', ?)", (expected,))

    def get(self, record_id):
        row = self.connection.execute("SELECT * FROM outbox WHERE record_id=?", (record_id,)).fetchone()
        return dict(row) if row else None

    def prepare(self, jobs):
        with self.connection:
            for job in jobs:
                previous = self.get(job["record_id"])
                if previous and previous["fingerprint"] != job["fingerprint"]:
                    raise ValueError("Outbox identity conflict")
                self.connection.execute("""INSERT OR IGNORE INTO outbox
                    (record_id,fingerprint,topic,message_key,payload,state,created_at)
                    VALUES (?,?,?,?,?,'prepared',?)""", (
                        job["record_id"], job["fingerprint"], job["topic"], job["key"],
                        json.dumps(job["payload"], ensure_ascii=False, allow_nan=False, sort_keys=True), utc_now()))

    def mark_sent(self, acknowledgements):
        with self.connection:
            for record_id, partition, offset in acknowledgements:
                self.connection.execute("""UPDATE outbox SET state='sent',sent_at=?,output_partition=?,output_offset=?
                    WHERE record_id=?""", (utc_now(), partition, offset, record_id))

    def close(self):
        self.connection.close()
