# Phase 11 — Spark Micro-Batch Performance Monitoring

**Status:** Code committed; requires fresh local tests, the one-time 3,000-event fixture run, and browser verification. No measured outcomes are claimed yet.

## Why a new fixture?

The completed 100K and 1M runs did not persist Spark's per-batch `StreamingQueryProgress`. Phase 11 measures new progress instead of retroactively fabricating those metrics. It uses a **new topic** `streamshield.microbatch_3k.v1` and **new output + checkpoint** under `outputs/spark_microbatch_3k_v1/`. No existing topics, reports, Spark checkpoints, PostgreSQL rows, or previous benchmarks are modified.

## Metrics

Each recorded Spark micro-batch includes:

- `batch_id`
- `num_input_rows`
- `input_rows_per_second` and `processed_rows_per_second` (Spark-reported)
- `trigger_execution_ms` and `add_batch_ms` from `durationMs`, when available
- UTC timestamp supplied by Spark

The job uses `maxOffsetsPerTrigger=500`, `availableNow=True`, the existing validation / 10-minute watermark / dedup / illustrative risk-rule logic, and checkpointed Parquet output. **Input rows are source rows, not accepted Parquet rows.** Batch rates do not equal event latency, p95/p99 latency, or production steady-state throughput.

## Step 1 — Pull and unit-test

On Mac, from your project directory:

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
python -m unittest discover -s tests -v
docker compose up -d
```

**Stop if tests fail** and share the failure output. Do not run the Spark job until tests pass.

## Step 2 — Publish the isolated fixture (run once only)

```bash
python -m streamshield.microbatch_fixture
```

Expected success fields: `attempted=3000`, `acknowledged=3000`, `failed=0`, `unflushed=0`. The producer creates the new topic if absent and **refuses to publish when its partitions already have nonzero offsets**. If a publish fails halfway through, preserve the topic and share the error; do not rerun blindly.

## Step 3 — Run Spark once

Check Java 17 and start the isolated Spark job:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/microbatch_spark.py
```

The Spark runner refuses to reuse an existing Phase 11 `accepted/`, `checkpoints/`, `progress_report.json`, or `run_result.json`. It verifies **3,000 persisted, distinct IDs**, requires all 3,000 Spark source rows to appear across captured batch reports, and requires at least two recorded micro-batches before writing successful metrics.

The saved files are:

- `outputs/spark_microbatch_3k_v1/progress_report.json` — actual batch metrics
- `outputs/spark_microbatch_3k_v1/run_result.json` — overall fixture verification

If the job fails, **do not delete** the checkpoint, accepted output, or topic; share the error.

## Step 4 — Display dashboard

```bash
docker compose --profile dashboard up -d --build
```

Open **http://localhost:8501** → **Spark Micro-Batches**.

The page reads the saved JSON report through the existing Docker read-only `./outputs:/app/outputs:ro` mount. It shows Spark-reported rates, trigger durations, and per-batch rows. It does not start Spark or modify Kafka.

## Safe stop

```bash
docker compose --profile dashboard stop dashboard
```

This stops only the optional dashboard, not Kafka/PostgreSQL. Never run `docker compose down -v`.

## Acceptance checklist

- New and existing unit tests pass locally
- Kafka fixture reports 3,000 acknowledgements, zero failures
- Spark runner produces `status=PASS` and recorded micro-batches (at least 2)
- Spark Micro-Batches page displays actual report and charts
- Existing 100K/1M outputs and checkpoints remain untouched

No phase should be marked verified until these are observed.
