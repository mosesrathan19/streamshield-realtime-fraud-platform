# Phase 5C — Isolated out-of-order and late-event test

**Status: verified locally November 25, 2026.** The follow-up verifier reported `PASS` with four unique persisted event IDs: `wm_on_time`, `wm_outorder`, `wm_seed_1`, and `wm_seed_2`. It reported `late_event_absent=true`. This is evidence for the specific controlled ordering scenario, not generalized late-data correctness.

The experiment deliberately avoids the existing 10,100-row Spark checkpoint and live transaction topic. It creates a **new single-partition Kafka topic** (`streamshield.watermark_test.v1`) and **new** Spark output and checkpoint directory (`outputs/spark_watermark_phase5`). Existing Kafka transactions, Parquet, and PostgreSQL data are untouched.

## What we're testing

Spark's `withWatermark("event_ts", "10 minutes")` with `dropDuplicatesWithinWatermark(["event_id"])`. The watermark uses the **maximum observed event time** minus a 10-minute delay, not the wall clock.

Send a seed stage, allow the first Spark job to complete, then send the follow-up stage:

| Stage | ID | UTC event time | Why |
|---|---|---|---|
| seed | `wm_seed_1` | 00:30 | seed transaction |
| seed | `wm_seed_2` | 00:31 | advances event-time maximum |
| followup | `wm_too_late` | 00:00 | older than watermark after seed |
| followup | `wm_outorder` | 00:25 | out of order but within 10-min allowance |
| followup | `wm_on_time` | 00:32 | new event maximum |
| followup | `wm_seed_1` | 00:30 | replayed ID; should be deduplicated |

The expected accepted IDs are `wm_seed_1`, `wm_seed_2`, `wm_outorder`, and `wm_on_time`. Note that **watermark advancement depends on Spark micro-batch progress**. If the expected IDs differ, inspect the actual result and query progress before drawing a conclusion; do not reset state.

## 1. Preflight and tests

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
python -m pip install -e '.[kafka]'
python -m unittest discover -s tests -v
docker compose ps
```

Do not proceed if the tests fail, Kafka is unhealthy, or `outputs/spark_watermark_phase5` already exists from a previous attempt.

## 2. Publish seed records to the isolated Kafka topic

```bash
python -m streamshield.watermark_fixture seed
```

Expected `attempted=2`, `acknowledged=2`, `failed=0`. The producer refuses to publish if the topic offset range does not match the intended stage, to guard against accidentally repeating a stage.

## 3. Process seed events with the isolated Spark checkpoint

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/spark_stream.py \
  --brokers 127.0.0.1:9092 \
  --topic streamshield.watermark_test.v1 \
  --output-dir outputs/spark_watermark_phase5 \
  --skip-windows
python -m streamshield.watermark_verify seed
```

Expected: PASS with 2 unique persisted IDs.

## 4. Publish late, out-of-order, on-time, and duplicate events

```bash
python -m streamshield.watermark_fixture followup
```

Expected `attempted=4`, `acknowledged=4`, `failed=0`.

## 5. Run Spark again with the *same isolated checkpoint*

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/spark_stream.py \
  --brokers 127.0.0.1:9092 \
  --topic streamshield.watermark_test.v1 \
  --output-dir outputs/spark_watermark_phase5 \
  --skip-windows
python -m streamshield.watermark_verify followup
```

Expected: PASS with 4 unique persisted IDs; `wm_too_late` should be absent and `wm_seed_1` should appear once.

## Interpretation

This is a local educational demonstration, not proof of robust late-data handling under all arrival patterns. The stateful watermark/dedup operator may drop events older than its event-time watermark, including unseen IDs. Therefore a 'late record dropped' means **it was excluded from the accepted Parquet sink**, not that an invalid or late-event quarantine was implemented. A separate DLQ/late-event audit remains future work. Do not send real payment data to this setup.

**Do not delete or alter:** `outputs/spark_phase3`, `streamshield.transactions.v1`, database volumes, existing checkpoints.
