"""Phase 4: replay-safe Spark Parquet -> PostgreSQL for synthetic data.

This is a batch/backfill sink fed by Phase 3's persisted streaming output;
it is not yet a continuous Spark foreachBatch streaming sink.
Requires optional psycopg and PySpark dependencies.
"""
from __future__ import annotations
import argparse
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

COLUMNS = (
    "event_id", "transaction_id", "account_id", "merchant_id",
    "merchant_category", "amount_cents", "currency", "country", "event_ts",
    "risk_high_amount", "risk_synthetic_country", "risk_flag"
)
INSERT_SQL = (
    "INSERT INTO transactions (" + ", ".join(COLUMNS) + ") VALUES ("
    + ", ".join(["%s"] * len(COLUMNS)) + ") ON CONFLICT (event_id) DO NOTHING"
)


def normalize_row(row) -> tuple:
    """Validate a Spark Row and make UTC timestamp explicit for psycopg."""
    data = row.asDict() if hasattr(row, "asDict") else dict(row)
    missing = [key for key in COLUMNS if data.get(key) is None]
    if missing:
        raise ValueError("missing required fields: " + ",".join(missing))
    # Spark serializes timestamps to Python using the host timezone.
    # Do not attach UTC to that naive datetime: on macOS/Pacific this shifts
    # Jan 1 midnight UTC back to Dec 31 and corrupts day-level reporting.
    # Derive the true UTC instant from Spark's timezone-independent epoch.
    epoch_us = data.get("event_ts_epoch_us")
    if type(epoch_us) is not int:
        raise ValueError("event_ts_epoch_us must be a Spark-derived integer")
    event_ts = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=epoch_us)
    if type(data["amount_cents"]) is not int or data["amount_cents"] <= 0:
        raise ValueError("amount_cents must be a positive integer")
    for key in ("risk_high_amount", "risk_synthetic_country", "risk_flag"):
        if type(data[key]) is not bool:
            raise ValueError(key + " must be boolean")
    if data["risk_flag"] != (
        data["risk_high_amount"] or data["risk_synthetic_country"]
    ):
        raise ValueError("inconsistent risk flags")
    return tuple(event_ts if key == "event_ts" else data[key] for key in COLUMNS)


def crash_after_commit(committed_batches: int, crash_after_batches: int) -> None:
    """Deterministic fault injection strictly AFTER a successful DB commit."""
    if crash_after_batches and committed_batches == crash_after_batches:
        raise RuntimeError(
            f"SIMULATED_FAILURE_AFTER_COMMIT: {committed_batches} committed batches"
        )


def load(parquet_dir: str, dsn: str, batch_size: int = 1000,
         crash_after_batches: int = 0) -> dict:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if crash_after_batches < 0:
        raise ValueError("crash_after_batches cannot be negative")
    from pyspark.sql import SparkSession, functions as F
    import psycopg

    spark = (SparkSession.builder.master("local[2]")
             .appName("StreamShieldPostgresLoad")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = (spark.read.parquet(parquet_dir).select(*COLUMNS)
                  .withColumn("event_ts_epoch_us", F.unix_micros("event_ts")))
        source_count = source.count()
        source_unique = source.select("event_id").distinct().count()
        source_alerts = source.filter(F.col("risk_flag")).count()
        if source_count != source_unique:
            raise ValueError("Parquet input has duplicate event_id values")
        schema_sql = (Path(__file__).resolve().parents[2] / "sql" / "001_schema.sql").read_text()
        inserted = 0
        committed_batches = 0
        with psycopg.connect(dsn, autocommit=True) as con:
            with con.cursor() as cur:
                cur.execute(schema_sql)
            # One DB transaction per chunk: retries cause no new duplicate rows.
            pending = []
            for row in source.toLocalIterator():
                pending.append(normalize_row(row))
                if len(pending) >= batch_size:
                    with con.transaction():
                        with con.cursor() as cur:
                            cur.executemany(INSERT_SQL, pending, returning=False)
                            # rowcount is the aggregate inserted count for executemany.
                            inserted += cur.rowcount
                    pending.clear()
                    committed_batches += 1
                    crash_after_commit(committed_batches, crash_after_batches)
                # save batch independent of original Spark partition sizes
            if pending:
                with con.transaction():
                    with con.cursor() as cur:
                        cur.executemany(INSERT_SQL, pending, returning=False)
                        inserted += cur.rowcount
                committed_batches += 1
                crash_after_commit(committed_batches, crash_after_batches)
            with con.cursor() as cur:
                cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE risk_flag) FROM transactions")
                total, total_alerts = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM daily_kpis")
                days = cur.fetchone()[0]
        result = {
            "source_records": source_count,
            "source_unique_event_ids": source_unique,
            "source_synthetic_risk_flags": source_alerts,
            "newly_inserted": inserted,
            "committed_batches": committed_batches,
            "database_total_transactions": total,
            "database_total_flagged": total_alerts,
            "database_daily_kpi_rows": days,
            "scope": "local replay-safe batch ingestion from previously persisted Spark Parquet"
        }
        print(json.dumps(result, indent=2))
        return result
    finally:
        spark.stop()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--parquet-dir", default="outputs/spark_phase3/accepted")
    p.add_argument("--dsn", default=os.environ.get(
        "STREAMSHIELD_PG_DSN",
        "postgresql://streamshield:streamshield_dev_only@127.0.0.1:5433/streamshield"))
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--crash-after-batches", type=int, default=0,
                   help="Phase 5 ONLY: simulate a failure AFTER N committed batches; 0 disables")
    a = p.parse_args()
    load(a.parquet_dir, a.dsn, a.batch_size, a.crash_after_batches)


if __name__ == "__main__":
    main()
