# Phase 4 — PostgreSQL analytics (local macOS)

**Status:** Docker configuration, SQL schema and loader committed. Actual PostgreSQL ingestion awaits local verification.

## Architecture / scope

`Kafka (Phase 2) → Spark streaming → Parquet (Phase 3) → replay-safe PostgreSQL loader → SQL views`.

This is a **batch/backfill loader from the persisted Spark output**, not yet a continuously running Spark `foreachBatch` PostgreSQL streaming sink. We use a durable, local PostgreSQL database so analytics outlive a Python process.

## Step 1 — Pull code and start PostgreSQL

From your existing project directory:

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
docker compose up -d
docker compose ps
```

Expect **both** `streamshield-kafka` and `streamshield-postgres` containers healthy. PostgreSQL listens only on host `127.0.0.1:5433` to avoid clashing with a local Postgres installation. Do not expose this local demo password or database to the internet. Docker Compose volume `postgres_data` keeps the data when containers restart.

## Step 2 — Install client in existing Spark environment

```bash
source .venv-spark/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
python -m pip install -e '.[postgres]'
```

You already installed PySpark 3.5.6 in `.venv-spark`. This optional extra installs the PostgreSQL client (`psycopg`).

## Step 3 — Load the existing 10,000 Spark records

```bash
python -m streamshield.postgres_load --parquet-dir outputs/spark_phase3/accepted
```

On the first successful run with a new database, expect approximately 10,000 new rows and 161 synthetic risk flags **if you are loading precisely the Phase 3 verified output**. The actual output is authoritative. The loader aborts if input `event_id` values are duplicated, required fields are missing, risk flags are inconsistent, or PostgreSQL constraints are violated.

Run the **exact same command again**. This replay should produce `newly_inserted: 0`; total stored rows and flagged counts should remain unchanged.

## Step 4 — Run SQL analytics

```bash
docker exec streamshield-postgres psql -U streamshield -d streamshield -c "SELECT count(*) AS transactions, count(*) FILTER (WHERE risk_flag) AS risk_alerts FROM transactions;"
docker exec streamshield-postgres psql -U streamshield -d streamshield -c "SELECT * FROM daily_kpis ORDER BY event_day_utc;"
docker exec streamshield-postgres psql -U streamshield -d streamshield -c "SELECT * FROM merchant_category_kpis ORDER BY flagged_transactions DESC;"
```

## Data model

`sql/001_schema.sql` defines:

- `transactions`: unique `event_id` and checks on amount, currency and risk consistency, indexed for time, category, account and flagged events.
- `fraud_alerts`: view over flagged synthetic transactions (not a separately inserted alert queue).
- `daily_kpis`: UTC date, transaction count, flagged count, total cents and flagged share.
- `merchant_category_kpis`: category counts, flagged counts and amounts.

## Reliability and limitations

- `ON CONFLICT (event_id) DO NOTHING` makes reloads safe for identical input: previously stored events remain unchanged. **A conflicting payload with the same ID is currently ignored**, not detected as data corruption; future hardening should flag payload mismatches.
- Each insert chunk runs in its own PostgreSQL transaction. Crash/retry can resume without duplicate rows; partial loading is possible until replay completes.
- `Spark.toLocalIterator()` streams partition data through the driver; it is suitable for this local 10K demonstration but not the final distributed 5M+ streaming architecture.
- Database totals can include earlier runs; use a fresh database for isolated acceptance counts.
- PostgreSQL credentials in Docker Compose are **development-only** and not appropriate for deployment.
- Real fraud classification, enterprise security, availability, and end-to-end exactly-once delivery are **not** claimed.

## Stop containers without deleting the database

```bash
docker compose down
```

**Avoid** `docker compose down -v` unless you intentionally want to delete the Kafka and PostgreSQL persistent data volumes.
