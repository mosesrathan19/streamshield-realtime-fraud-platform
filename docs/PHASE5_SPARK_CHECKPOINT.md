# Phase 5B — Spark checkpoint restart test

**Status: code and procedure committed; test has not yet been executed on the user's Mac.**

This is a **graceful stop/restart** checkpoint test. It verifies the existing Spark Structured Streaming query resumes from previously committed Kafka offsets, processes a new batch and retains unique event IDs in the existing Parquet output. It is **not** a SIGKILL/crash-recovery test yet.

## Preflight: do not reset existing state

Use the existing Kafka topic `streamshield.transactions.v1`, and the existing Spark output/checkpoints at `outputs/spark_phase3`. **Do not delete checkpoints, Parquet files, Kafka topics, or Docker volumes.** These instructions assume Phase 3 already persisted the original 10,000 events.

Check that no other Spark job is actively writing to this directory. Run from your project root:

```bash
git pull
source .venv-spark/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
docker compose ps
python -m unittest discover -s tests -v
python -m streamshield.checkpoint_verify --expected-count 10000
```

If the count is **not exactly 10,000**, stop and share the output. The test was designed to extend this verified baseline, not to overwrite or repair unknown checkpoint state.

## Step 1: replay the existing checkpoint without new events

Run the **same** Spark streaming job against the **same checkpoint and output paths**:

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/spark_stream.py \
  --brokers 127.0.0.1:9092 \
  --output-dir outputs/spark_phase3
```

Then rerun:

```bash
python -m streamshield.checkpoint_verify --expected-count 10000
```

The verifier checks row count and uniqueness and reports the risk-flag count. A correct no-new-data restart leaves 10,000 unique events. If Kafka has received other events in the meantime, the output may differ; do not delete state to force the expected number.

## Step 2: produce 100 genuinely new event IDs

The existing producer's `--start` flag changes the deterministic event index, so IDs **10000 through 10099** are new and have event timestamps following the original 10,000.

```bash
python -m pip install -e '.[kafka]'
python -m streamshield.kafka_client --brokers 127.0.0.1:9092 produce --start 10000 --count 100
```

Expected: `attempted=100`, `acknowledged=100`, `failed=0`. Do not publish the new batch more than once for this first test.

## Step 3: restart Spark with the SAME checkpoint

Run the Step 1 `spark-submit` command again. Once it completes, verify:

```bash
python -m streamshield.checkpoint_verify --expected-count 10100 --check-new-range
```

Expected `status: PASS`, `persisted_events: 10100`, `unique_event_ids: 10100`, `new_event_ids_verified: 100`. The synthetic risk count will be printed and should be compared with the 10,000-row baseline (161) and checked against generated events; do **not** assume a fixed new alert count.

## Step 4: repeat restart without new messages

Run the Step 1 `spark-submit` command again, then the Step 3 verifier again. Expected: unchanged 10,100 unique events, demonstrating a no-op replay with persisted Kafka offsets. You can run this only after Step 3 passes.

## Boundaries of the claim

This demonstrates **checkpoint-based graceful restart** and **persisted output uniqueness** for a local single-node Spark/Kafka setup. It does not test process kill in mid-batch, lost network, broker failover, late-message handling beyond the watermark, schema quarantine, or global exactly-once semantics. Keep this honest in interviews and on your resume.

**Safety:** The checks are read-only; the Kafka producer appends 100 synthetic events; Spark appends Parquet data. Your existing PostgreSQL databases remain unchanged because the Spark job does not load data into PostgreSQL.
