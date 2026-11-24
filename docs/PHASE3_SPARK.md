# Phase 3 — PySpark Structured Streaming + Kafka (local macOS)

**Status:** Source code committed; not yet executed on your Mac. Phase 2 Kafka producer/consumer and local persisted alerts have already been verified.

## What this adds

- Kafka-backed **Spark Structured Streaming** micro-batches reading `streamshield.transactions.v1`.
- JSON schema validation (invalid rows are filtered, **not yet quarantined**).
- Event-time parsing in UTC, 10-minute watermark and stateful `event_id` deduplication.
- Two independently checkpointed streams: persisted accepted transactions as Parquet; live-updating one-minute event-time window counts by synthetic risk flag printed to console.
- A persisted-result summary after `availableNow` catches up with currently retained Kafka data.

**Limitations:** This is local Spark, not a multi-node cluster. The window console sink is not a durable analytics table. State is bounded by the watermark, so very late duplicates beyond the retained state window are not guaranteed to be suppressed. Each query has its own checkpoint and state. This stage is **not** certified exactly-once across external systems.

## Prerequisites

- Phase 2's local Kafka broker must be healthy: `docker compose ps`
- Spark 3.5.6 with matching Kafka connector; Java 17 recommended
- Python 3.10 or 3.11 recommended for Spark 3.5.6
- Several GB of free RAM and working network access to Maven repositories on first run

**Important:** Your existing `.venv` may have a newer Python runtime. Check `python --version` first. For Python 3.13, create a separate Python 3.11 venv rather than forcing Spark 3.5.6 into an unsupported environment.

## Install (existing Python 3.10/3.11 environment)

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv/bin/activate
python --version
java -version
python -m pip install 'pyspark==3.5.6'
```

If Python reports 3.12+ or Java is not available, stop and share the version output before continuing. Java 17 is recommended; verify with `java -version`.

## Run locally

Use a **fresh directory** for this first run; the directory persists Spark state and prevents accidental reprocessing with the same checkpoint.

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/spark_stream.py \
  --brokers 127.0.0.1:9092 \
  --output-dir outputs/spark_phase3
```

The first run may download several Maven dependencies. Spark will read all **retained** Kafka messages starting from the earliest offsets for new checkpoints. This may include replayed messages from Phase 2 and can be more than 10,000.

It writes accepted transactions to `outputs/spark_phase3/accepted`, stores state under `outputs/spark_phase3/checkpoints`, prints updating one-minute windows, and reports the deduplicated persisted event count at completion.

Re-running with the same checkpoint should not reprocess already committed Kafka offsets; new messages are processed if present. **Don't delete checkpoints merely to force a demo run**, because doing so can duplicate persisted results.

## Troubleshooting

- `DATA_SOURCE_NOT_FOUND: kafka`: ensure you invoked **spark-submit with --packages** and the connector version matches Spark 3.5.6.
- `Connection refused`: `docker compose ps` and use IPv4 `127.0.0.1:9092`.
- `UnsupportedClassVersionError` or JVM startup error: check installed Java version. Use JDK 17.
- `Python worker failed`: check Python 3.10/3.11, PATH, and `PYSPARK_PYTHON` if multiple interpreters are installed.
- No window rows: inspect whether Kafka retained input, whether first offsets are already checkpointed, and the watermark/window behavior. Output mode is **update**, so displayed rows may be revised by later batches.

## What counts as Phase 3 completion

We will run the Spark job on your Mac, inspect the final persisted counts, verify a second checkpointed run, and record actual results in this repository. Until then, Phase 3 is **implemented in code, awaiting runtime verification**.

Official references: [Spark 3.5.6 Kafka integration](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html), [Structured Streaming guide](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html).
