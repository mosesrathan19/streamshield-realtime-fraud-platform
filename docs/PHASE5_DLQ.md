# Phase 5D — Malformed event quarantine / Dead-Letter Queue demonstration

**Status: code committed; runtime test on Mac pending.**

## Architecture / isolation

This test uses dedicated single-partition Kafka input topic `streamshield.dlq_test.v1` and separate Spark checkpoint directories under `outputs/spark_dlq_phase5`. It **does not touch** `streamshield.transactions.v1`, the verified 10,100-row Parquet data, or PostgreSQL.

Six synthetic messages are published:
- Two valid transactions.
- One negative amount.
- One missing account ID.
- One invalid timestamp.
- One malformed JSON string.

Spark writes **accepted Parquet rows** to `outputs/spark_dlq_phase5/accepted` and **rejected Parquet rows** to `outputs/spark_dlq_phase5/rejected`, each with an independent checkpoint.

The rejected dataset acts as a **local durable DLQ/quarantine equivalent**, not a Kafka DLQ topic: Kafka's reserved `streamshield.invalid.v1` topic is not populated here. Rejected records store only a reason, optional event ID, topic, partition and offset. They **do not store raw payloads**. This makes the test safer but means raw messages can only be recovered while Kafka retains them; a complete production quarantine policy would need durable redacted payload handling, access controls, and retention rules.

## Step 1 — Pull code and run unit tests

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
python -m unittest discover -s tests -v
docker compose ps
```

Stop if tests fail or Kafka is not healthy. Do not delete or overwrite outputs.

## Step 2 — Publish six synthetic messages (once)

```bash
python -m streamshield.dlq_fixture
```

Expected: `attempted=6`, `acknowledged=6`, `failed=0`, `unflushed=0`. The fixture **refuses to publish again** if its topic already contains messages, guarding against accidental duplicate tests.

## Step 3 — Spark routes accepted and rejected messages

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/dlq_stream.py \
  --brokers 127.0.0.1:9092 \
  --output-dir outputs/spark_dlq_phase5
```

Expected final totals: `accepted=2`, `rejected=4`, `total_routed=6`.

## Step 4 — Independently verify exact routing and Kafka offsets

```bash
python -m streamshield.dlq_verify
```

Expected `status=PASS`, `accepted=2`, `rejected=4`, `reconciled_kafka_offsets=6`, and one count each for:
- `invalid_amount`
- `missing_account_id`
- `invalid_event_time`
- `missing_or_malformed_event_id`

If the verifier fails, **do not erase checkpoints or republish**. Share the output, and inspect the actual records and Spark logs first.

## Step 5 — Replay test (only after Step 4 passes)

Run the identical `spark-submit` command again, then rerun `python -m streamshield.dlq_verify`. Counts should remain 2 accepted and 4 rejected because the checkpoints remember committed source offsets.

## What this proves and does not prove

A passing local test verifies basic malformed-data handling, isolated durable Parquet quarantine, reconciliation by Kafka coordinates, and no-op checkpoint replay in this fixture. It does not prove that rejected data was published to a Kafka DLQ topic, atomic cross-sink commits, forced crash recovery, complex semantic validation, or exactly-once behavior across services.

**Do not upload real transaction data** to this educational demo. Stored Kafka offsets are references, not durable archived copies of message bodies.
