"""Read-only data access for the local StreamShield Streamlit dashboard.

PostgreSQL contains only the previously backfilled transactions, not the 100K/1M
Spark benchmark datasets. Benchmarks are loaded from separate saved JSON files.
No SQL writes, Spark jobs, or Kafka consumption occurs in dashboard code.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
import json
import os

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DSN = "postgresql://streamshield:streamshield_dev_only@127.0.0.1:5433/streamshield_utc"
BENCHMARKS = {
    "100K": {
        "spark": ROOT / "outputs/spark_benchmark_100k_v2/benchmark_result.json",
        "producer": None,
    },
    "1M": {
        "spark": ROOT / "outputs/spark_benchmark_1m_v1/benchmark_result.json",
        "producer": ROOT / "outputs/spark_benchmark_1m_v1/producer_result.json",
    },
}
# This 100K Kafka producer figure was user-observed but not saved to JSON
# because the older producer implementation only printed the result.
KNOWN_100K_PRODUCER = {"producer_elapsed_seconds": 0.987,
                       "acknowledged_events_per_second": 101282.7,
                       "acknowledged": 100000}


def load_json(path: Path | None) -> dict | None:
    if path is None or not path.is_file():
        return None
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def benchmarks() -> list[dict]:
    results = []
    for label, paths in BENCHMARKS.items():
        spark = load_json(paths["spark"])
        if spark is None:
            continue
        producer = load_json(paths["producer"])
        if label == "100K" and producer is None:
            producer = KNOWN_100K_PRODUCER
        results.append({
            "run": label,
            "transactions": int(spark["persisted_events"]),
            "unique_ids": int(spark["distinct_event_ids"]),
            "synthetic_flags": int(spark["synthetic_risk_flags"]),
            "spark_seconds": float(spark["spark_query_elapsed_seconds"]),
            "spark_events_per_sec": float(spark["persisted_events_per_query_second"]),
            "producer_seconds": float(producer["producer_elapsed_seconds"]) if producer else None,
            "producer_events_per_sec": float(producer["acknowledged_events_per_second"]) if producer else None,
            "producer_source": "reported earlier in Terminal" if label == "100K" else "saved JSON",
            "source_topic": spark["source_topic"],
        })
    return results


def postgres_snapshots(dsn: str | None = None) -> dict:
    """Use a READ ONLY transaction for all dashboard queries."""
    import psycopg
    connection = dsn or os.environ.get("STREAMSHIELD_PG_DSN", DEFAULT_DSN)
    with psycopg.connect(connection, connect_timeout=4, options="-c default_transaction_read_only=on") as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), COUNT(*) FILTER (WHERE risk_flag), "
                "COALESCE(SUM(amount_cents),0), COUNT(DISTINCT account_id) "
                "FROM transactions"
            )
            total, flagged, cents, accounts = cur.fetchone()
            cur.execute(
                "SELECT event_day_utc, transactions, flagged_transactions, "
                "amount_cents, flagged_pct FROM daily_kpis ORDER BY event_day_utc"
            )
            daily = [
                {"day": str(day), "transactions": int(t), "flags": int(f),
                 "amount_usd": float(c)/100, "flagged_pct": float(p)}
                for day, t, f, c, p in cur.fetchall()
            ]
            cur.execute(
                "SELECT merchant_category, transactions, flagged_transactions, "
                "amount_cents, flagged_pct FROM merchant_category_kpis "
                "ORDER BY transactions DESC"
            )
            categories = [
                {"category": category, "transactions": int(t), "flags": int(f),
                 "amount_usd": float(c)/100, "flagged_pct": float(p)}
                for category, t, f, c, p in cur.fetchall()
            ]
            cur.execute(
                "SELECT COUNT(*) FILTER (WHERE risk_high_amount), "
                "COUNT(*) FILTER (WHERE risk_synthetic_country), "
                "COUNT(*) FILTER (WHERE risk_high_amount AND risk_synthetic_country) "
                "FROM transactions"
            )
            amount_flags, country_flags, overlap = cur.fetchone()
    return {
        "total": int(total), "flagged": int(flagged), "amount_usd": float(cents)/100,
        "accounts": int(accounts),
        "daily": daily, "categories": categories,
        "risk_reasons": {
            "High amount": int(amount_flags),
            "Synthetic country": int(country_flags),
            "Both (overlap)": int(overlap),
        },
    }
