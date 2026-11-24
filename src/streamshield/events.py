"""Deterministic, synthetic, streaming-friendly payment event contract."""
from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
COUNTRIES = ("US", "CA", "GB", "DE", "AU", "BR", "IN", "FR")
MERCHANTS = ("grocery", "travel", "electronics", "gaming", "fashion", "dining")


def event_at(i: int, seed: int = 42) -> dict:
    """Generate the same event for a given index/seed, independent of chunk sizes."""
    if i < 0:
        raise ValueError("event index must be nonnegative")
    # Multiplicative hashing gives reproducible variation without mutable RNG state.
    h = ((i + 1) * 2654435761 + seed * 1013904223) & 0xFFFFFFFF
    account = (h // 31) % 50_000
    # A deterministic high-velocity demonstration cohort.
    high_velocity = i % 97 == 0
    if high_velocity:
        account = i % 7
    value = 100 + (h % 49_900)
    if i % 113 == 0:
        value = 250_000 + (h % 450_000)
    country = COUNTRIES[(h // 101) % len(COUNTRIES)]
    if i % 137 == 0:
        country = "ZZ"  # invented country code used only for a synthetic rule
    return {
        "event_id": f"evt_{i:012d}",
        "transaction_id": f"txn_{i:012d}",
        "account_id": f"acct_{account:06d}",
        "merchant_id": f"merchant_{(h // 11) % 4000:04d}",
        "merchant_category": MERCHANTS[(h // 71) % len(MERCHANTS)],
        "amount_cents": value,
        "currency": "USD",
        "country": country,
        "event_time": (BASE + timedelta(milliseconds=i * 30)).isoformat(),
        "event_type": "payment_authorized",
        "schema_version": 1,
        "synthetic_label": bool(high_velocity or i % 113 == 0 or i % 137 == 0),
    }


def events(count: int, seed: int = 42, start: int = 0) -> Iterator[dict]:
    if count < 0 or start < 0:
        raise ValueError("count and start must be >= 0")
    for i in range(start, start + count):
        yield event_at(i, seed)


def write_jsonl(path: str | Path, count: int, seed: int = 42, start: int = 0) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for item in events(count, seed, start):
            file.write(json.dumps(item, separators=(",", ":")) + "\n")
    return count
