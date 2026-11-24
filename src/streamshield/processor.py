"""Bounded-memory local consumer with transactional SQLite deduplication.

Durable exactly-once effects for event_id within one SQLite database, provided
both bookkeeping and derived alert write commit in the same transaction.
This is NOT end-to-end exactly-once Kafka/Spark processing.
"""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from .rules import evaluate, validate

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS processed_events (
  event_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  event_time TEXT NOT NULL,
  amount_cents INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS risk_alerts (
  event_id TEXT PRIMARY KEY REFERENCES processed_events(event_id),
  account_id TEXT NOT NULL,
  reasons_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rejected_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_line INTEGER NOT NULL,
  error TEXT NOT NULL
);
"""


def connect(path: str | Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript(SCHEMA)
    return db


def process_jsonl(input_path: str | Path, database_path: str | Path, batch_size: int = 2000) -> dict:
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    stats = {"read": 0, "inserted": 0, "duplicate": 0, "rejected": 0, "alerts": 0}
    db = connect(database_path)
    try:
        with open(input_path, encoding="utf-8") as stream:
            line_no = 0
            while True:
                lines = [stream.readline() for _ in range(batch_size)]
                lines = [line for line in lines if line]
                if not lines:
                    break
                # Commit event IDs and alert effects together once per chunk.
                # Failure before commit rolls back this chunk; replay skips prior chunks.
                with db:
                    for line in lines:
                        line_no += 1
                        stats["read"] += 1
                        try:
                            event = json.loads(line)
                            validate(event)
                            reasons = evaluate(event)
                            cur = db.execute(
                                "INSERT OR IGNORE INTO processed_events (event_id,account_id,event_time,amount_cents) VALUES (?,?,?,?)",
                                (event["event_id"], event["account_id"], event["event_time"], event["amount_cents"]),
                            )
                            if cur.rowcount == 0:
                                stats["duplicate"] += 1
                            else:
                                stats["inserted"] += 1
                                if reasons:
                                    db.execute("INSERT INTO risk_alerts VALUES (?,?,?)", (event["event_id"], event["account_id"], json.dumps(reasons)))
                                    stats["alerts"] += 1
                        except (ValueError, TypeError, KeyError, json.JSONDecodeError) as err:
                            # Never store raw payloads in error reporting.
                            db.execute("INSERT INTO rejected_events(source_line,error) VALUES (?,?)", (line_no, type(err).__name__))
                            stats["rejected"] += 1
        assert stats["read"] == stats["inserted"] + stats["duplicate"] + stats["rejected"]
        return stats
    finally:
        db.close()
