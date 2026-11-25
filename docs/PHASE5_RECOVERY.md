# Phase 5 — Failure injection and recovery (development test)

**Current status:** PostgreSQL post-commit failure injection and manual recovery verified locally on November 25, 2026. After an intentional failure at two committed 1,000-row batches, the database contained 2,000 transactions and 29 synthetic risk flags. Restarting without injection inserted the remaining 8,000, resulting in 10,000 transactions and 161 synthetic risk flags. Kafka/Spark process termination, checkpoint recovery, delayed events and DLQ tests remain future Phase 5 work.

## Goal

Demonstrate the *at-least-once delivery, idempotent effect* pattern:
1. Process a batch of synthetic transactions.
2. Commit a portion to PostgreSQL.
3. Simulate a process failure immediately after the committed batch.
4. Restart and replay the **complete** Parquet dataset.
5. Confirm exactly one durable row per `event_id` and no repeated synthetic risk flags.

This isn't a network failure and doesn't establish multi-service exactly-once semantics. It tests durable storage replay after a controlled process exception.

## Safety

**Do not stop Kafka, Docker volumes, or your production-like Phase 4 demo database.** These commands only create and use a *new* database called `streamshield_recovery_v1`. They do not delete data. The failure is generated deliberately by the loader; seeing `SIMULATED_FAILURE_AFTER_COMMIT` is the expected test outcome.

### Step 1 — Get changes and run tests

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
python -m unittest discover -s tests -v
docker compose ps
```

Both Kafka and PostgreSQL should be healthy, although this first test only uses PostgreSQL and Spark Parquet.

### Step 2 — Create an isolated recovery database

```bash
docker exec streamshield-postgres createdb -U streamshield -O streamshield streamshield_recovery_v1
```

If this database already exists, stop and use a new distinct name rather than overwriting anything. For example change `recovery_v1` to `recovery_v2` in both the create and load commands.

### Step 3 — Intentionally fail after 2 committed batches

```bash
python -m streamshield.postgres_load \
  --parquet-dir outputs/spark_phase3/accepted \
  --dsn 'postgresql://streamshield:streamshield_dev_only@127.0.0.1:5433/streamshield_recovery_v1' \
  --batch-size 1000 \
  --crash-after-batches 2
```

**Expected**: traceback ending in `RuntimeError: SIMULATED_FAILURE_AFTER_COMMIT: 2 committed batches`. The injected exception happens **after** the second database commit; it isn't an accidental failure.

Verify the partial state:

```bash
docker exec streamshield-postgres psql -U streamshield -d streamshield_recovery_v1 -c "SELECT COUNT(*) AS stored, COUNT(*) FILTER (WHERE risk_flag) AS flagged FROM transactions;"
```

Expected `stored=2000`, with a partial synthetic-risk count. The exact partial risk count depends on Spark partition iteration order and is not fixed.

### Step 4 — Resume with the full input, injection disabled

```bash
python -m streamshield.postgres_load \
  --parquet-dir outputs/spark_phase3/accepted \
  --dsn 'postgresql://streamshield:streamshield_dev_only@127.0.0.1:5433/streamshield_recovery_v1' \
  --batch-size 1000
```

Expected: source 10,000, newly inserted 8,000, PostgreSQL total 10,000, flagged 161. Existing rows remain unchanged due to the unique `event_id` index and `ON CONFLICT DO NOTHING`.

### Step 5 — Prove repeated recovery is safe

Run the identical Step 4 load again; expect `newly_inserted=0`, total 10,000 and flagged 161.

Verify the final data with independent SQL:

```bash
docker exec streamshield-postgres psql -U streamshield -d streamshield_recovery_v1 -c "SELECT COUNT(*) AS total, COUNT(DISTINCT event_id) AS unique_ids, COUNT(*) FILTER (WHERE risk_flag) AS flagged FROM transactions;"
docker exec streamshield-postgres psql -U streamshield -d streamshield_recovery_v1 -c "SELECT * FROM daily_kpis ORDER BY event_day_utc;"
```

Expected 10,000 unique IDs, 161 risk flags, correct 2026-01-01 UTC day.

## Interpretation & limits

- Demonstrates recovery **after** a chunk was committed. It does not yet inject a fault *inside an open transaction*, SIGKILL Spark, restart Kafka or test true broker failover.
- The current sink loads Spark Parquet through a local Python driver rather than streaming directly from Kafka into PostgreSQL.
- No automatic orchestrator restarts this loader. The rerun is deliberately manual.
- The demo's unique index protects from identical duplicate events, but conflicting payloads with the same ID are silently ignored; this limitation remains.
- All results must be observed locally before Phase 5 is marked fully verified.
