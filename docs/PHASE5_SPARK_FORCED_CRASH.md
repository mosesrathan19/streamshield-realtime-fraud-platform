# Phase 5E — Real Spark driver SIGKILL and checkpoint recovery

**Status: locally verified November 25, 2026 (partial and full recovery).** Following the isolated forced-interruption test, the independent verifier reported `PASS` with 100 persisted events, 100 unique event IDs and 100 unique Kafka offsets. Restarting the Spark job in recover mode then produced `PASS` with all 300 expected events, 300 unique IDs and 300 accounted-for Kafka offsets. The user reported running the final no-op recovery verification; its JSON output was not pasted, so its exact counts remain uninspected.

This is a deliberately destructive test of **one isolated Spark process**, not your existing StreamShield pipelines. It uses its own single-partition Kafka topic, 300 synthetic events, and the checkpointed Parquet output `outputs/spark_crash_phase5`.

### What is different from Phase 5B?

Phase 5B gracefully stopped Spark after completing its work. Here, a separate Spark driver running `src/streamshield/crash_stream.py` calls **SIGKILL** on *itself* once it observes completed micro-batches. There is no graceful `spark.stop()` on this path. A second invocation resumes the **same** Kafka query and Parquet checkpoint. This proves recovery for a driver crash after successful batches, not necessarily a crash during an uncommitted batch.

## Step 1: Update code and verify Docker

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
python -m unittest discover -s tests -v
docker compose ps
```

**Safety:** Do not change the topic or output path. The script only uses `streamshield.crash_test.v1` and `outputs/spark_crash_phase5`. It must not be run concurrently with any other job that uses that checkpoint. Do not delete Kafka topics, checkpoints, Parquet outputs or Docker volumes.

## Step 2: Send the 300 events to the isolated topic exactly once

```bash
python -m streamshield.crash_fixture
```

Expected: `attempted=300`, `acknowledged=300`, `failed=0`, `unflushed=0`. The fixture refuses to republish if Kafka's topic already has any offsets.

## Step 3: Start Spark and deliberately terminate the driver

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/crash_stream.py \
  --mode crash \
  --confirm-isolated-crash
```

Spark reads at most 50 Kafka records per micro-batch. The driver polls `lastProgress` and calls `SIGKILL` after it observes a completed batch with batch ID at least 1. Expect a terminal line similar to:

```text
EXPECTED FORCED SPARK DRIVER TERMINATION: SIGKILL
```

The process should terminate with a nonzero status and may show `Killed: 9`; this is **intentional**. If it exits normally without this line, do not claim the crash test succeeded.

**Note:** SIGKILL is a sudden process termination. Existing Spark log output may be noisy. The crash may occur after more than 100 records were persisted if Spark advances another micro-batch while the monitor polls; don't assume an exact partial count.

## Step 4: Verify data persisted before the crash

```bash
python -m streamshield.crash_verify
```

Expected: `status=PASS`, with **between 1 and 299** persisted records and the same number of unique IDs and source offsets. The partial count is intentionally not fixed.

If the crash occurred before any files were committed, or after all 300 were committed, this controlled test is inconclusive; share the outputs rather than resetting data.

## Step 5: Restart from the SAME checkpoint

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/crash_stream.py \
  --mode recover
```

Then verify independently:

```bash
python -m streamshield.crash_verify --full
```

Expected: `status=PASS`, `persisted_events=300`, `unique_event_ids=300`, `unique_kafka_offsets=300`. This verifies that all 300 Kafka offsets have one persisted effect, with no duplicates, after abrupt driver termination and manual restart.

## Step 6: Prove a subsequent restart has no duplicate effects

Repeat Step 5 (recover mode) and the full verifier. Expect unchanged counts. Do not republish the fixture.

## Scope and limitations

This is a **local Spark driver failure test**, not an actual JVM crash, Kafka broker outage, multi-machine failover, or end-to-end exactly-once guarantee. The test uses Spark's checkpoint-aware built-in Parquet sink; outcomes do not automatically carry over to arbitrary external sinks such as PostgreSQL. Force-killing a process after committed micro-batches does not yet test mid-commit failure. Never run this crash mode against the main 10,100-event Spark checkpoint.

The partial and full recovery results above were confirmed by the user's local verifier output. The user reported completing an additional no-new-message restart, but its verifier output has not been shared.
