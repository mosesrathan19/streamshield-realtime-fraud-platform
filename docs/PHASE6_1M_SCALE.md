# Phase 6D — Isolated 1,000,000-event Kafka -> Spark benchmark

**Status: implementation committed; local execution and results not yet verified.**

## Purpose and isolation

Benchmark a fresh 1M-event synthetic transaction workload, distinct from the successful 100K v1/v2 trials. This test uses:

- Dedicated Kafka topic `streamshield.benchmark_1m.v1` with **six partitions**
- Deterministic synthetic event indices 2,000,000–2,999,999, generated on the fly
- Existing Spark 3.5.6 validation, 10-minute watermark, event-ID deduplication and synthetic risk-rule logic
- Separate Parquet output `outputs/spark_benchmark_1m_v1/accepted`
- Separate Spark checkpoint `outputs/spark_benchmark_1m_v1/checkpoints/accepted`
- Automatically saved local reports: `producer_result.json` and `benchmark_result.json`

**The primary 10,100-event data and 100K v1/v2 benchmarks are not touched.**

## Important resource check

1M Kafka messages plus Spark shuffle/state/checkpoints/Parquet writes can require additional disk and memory. The producer will refuse to run with less than **5 GiB free host filesystem space**, but this does **not** check Docker Desktop's virtual disk capacity or guarantee sufficient RAM. Stop and review rather than deleting existing outputs if disk is scarce.

On a Mac:
```bash
df -h .
docker system df
```
`docker system df` reports Docker-managed usage, not necessarily all free Docker VM capacity. Also check Docker Desktop > Settings > Resources if needed. Don't run other heavy streaming jobs simultaneously.

## Step 1 — Update code, environment, and test

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
python -m unittest discover -s tests -v
docker compose ps
python -m streamshield.scale_1m_fixture --check-space
df -h .
```

Expected preflight JSON: `sufficient: true`. Kafka container must be up; unit tests must pass. **Do not continue** if there are failures.

## Step 2 — Publish 1M events (run exactly once)

```bash
python -m streamshield.scale_1m_fixture
```

The producer refuses to use a nonempty source topic. Expected successful counters: `attempted=1000000`, `acknowledged=1000000`, `failed=0`, `unflushed=0`, `partitions=6`. Exact elapsed seconds and throughput are measured, not predetermined.

Report saved to `outputs/spark_benchmark_1m_v1/producer_result.json`. In this benchmark timing scope, Kafka producer JSON generation, send, acknowledgement callbacks and flush are included; topic creation and preflight are excluded.

If publishing fails or the acknowledgement count is short, **do not rerun blindly**. Share the error and inspect the topic.

## Step 3 — Process with existing Spark enrichment logic (run once)

Only after full producer acknowledgement:

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/scale_1m_spark.py
```

The script refuses to overwrite existing benchmark Parquet, checkpoint or Spark JSON report. It saves `outputs/spark_benchmark_1m_v1/benchmark_result.json` if the Spark query completes and persists 1M unique records.

The `spark_query_elapsed_seconds` interval includes the Spark query's `start()`, all `availableNow` micro-batch work, checkpointing and Parquet completion. It **excludes** SparkSession startup, the separate producer step and post-run verification. These rates are **not** an end-to-end synchronous throughput or per-event latency.

If Spark encounters an out-of-memory failure, JVM error, unexpected event loss or timeout, do not delete any checkpoint or redo a one-shot benchmark. Preserve evidence for diagnosis.

## Step 4 — Read saved metrics (read-only)

```bash
cat outputs/spark_benchmark_1m_v1/producer_result.json
cat outputs/spark_benchmark_1m_v1/benchmark_result.json
```

## Step 5 — Independent Kafka-offset and event-ID reconciliation

```bash
python -m streamshield.scale_1m_verify
```

Expected after a fully successful 1M test: `status=PASS`, `total_events=1000000`, `unique_event_ids=1000000`, `invalid_event_ids=0`, `reconciled_kafka_offsets=1000000` and no errors. The verifier confirms that each of the six Kafka partitions has a contiguous offset range from zero, with no duplicate persisted coordinates, and that all event IDs fall in the expected unique 1M range.

**No performance results are asserted until the actual Mac outputs have been inspected.**

## Interpretation and limitations

The validated 100K v2 comparison baseline is **101,282.7 acknowledged events/sec** from the Kafka producer, and **11,379.5 persisted events/sec** over an **8.788s** Spark query interval; these are **separate stage timings** from a single macOS test. Compare 1M to these scoped measurements without treating either rate as steady-state or end-to-end latency. The 1M test is an educational, local, one-node benchmark: it doesn't assess p50/p95/p99 latency, production cluster scaling, memory/CPU utilization, or broker failure scenarios.

**Observed 1M Kafka producer result (November 25, 2026):** Topic `streamshield.benchmark_1m.v1`, 6 partitions; attempted **1,000,000**, acknowledged **1,000,000**, failed **0**, unflushed **0**; elapsed **5.398 s**, acknowledged producer throughput **185,241.2 events/sec**; host free space before publishing **26.53 GiB**. The producer saved a local `outputs/spark_benchmark_1m_v1/producer_result.json` file. Spark Parquet reconciliation passed; measured Spark query duration and throughput remain pending review of the saved JSON. Producer throughput is a separate stage measurement, not end-to-end streaming throughput.


**Verified 1M Spark/Parquet reconciliation (November 25, 2026):** Local read-only verifier returned `PASS` with **1,000,000 persisted events**, **1,000,000 distinct event IDs**, **0 invalid IDs**, **1,000,000 reconciled Kafka offsets**, and `errors=[]`. Partition row counts (0–5): **168,159 / 165,140 / 165,021 / 171,692 / 166,078 / 163,910**; each partition's offset range started at 0 and ended at rows−1, with all offsets unique. The Spark processing report `benchmark_result.json` has now been shared and verified; see the 1M benchmark result table below. This is isolated local correctness verification, not proof of end-to-end exactly-once semantics.


## Verified Phase 6D 1M streaming benchmark results (November 25, 2026)

The locally saved `outputs/spark_benchmark_1m_v1/benchmark_result.json` was inspected after the independent read-only Kafka offset and event-ID reconciliation returned **PASS**.

| Metric | 100K v2 measured | 1M v1 measured |
|---|---:|---:|
| Events published / persisted | 100,000 / 100,000 | 1,000,000 / 1,000,000 |
| Kafka producer elapsed | 0.987 s | **5.398 s** |
| Acknowledged producer rate | 101,282.7 events/s | **185,241.2 events/s** |
| Spark availableNow query elapsed | 8.788 s | **19.433 s** |
| Spark persisted events/query second | 11,379.5 | **51,459.2** |
| Synthetic illustrative risk flags | 1,609 | **16,084** |
| Unique persisted event IDs | 100,000 | **1,000,000** |
| Reconciled Kafka offsets | 100,000 | **1,000,000** |
| Independent integrity check | PASS | **PASS** |

Spark rate was **4.52×** higher in the 1M run, but these are two single runs at different workload sizes and not repeated steady-state throughput experiments. The Spark timing includes query start, `availableNow` processing, checkpointed Parquet completion; it excludes Kafka publishing, SparkSession startup and post-run verification. Producer and Spark figures represent separate measurement scopes, not end-to-end latency or a single concurrent throughput rate. These checks demonstrate local result completeness/uniqueness for this synthetic fixture; they do not imply exactly-once across external sinks.

