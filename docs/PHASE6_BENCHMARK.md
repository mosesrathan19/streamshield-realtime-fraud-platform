# Phase 6 — Reproducible performance benchmarking

**Status:** Phase 6 local JSONL-to-SQLite 100K baseline measured on Mac November 25, 2026. Kafka/Spark streaming benchmarks, p95 latency, resource profiling and million-event scale runs remain pending.

## Goals

Measure throughput and resources honestly across distinct parts of StreamShield. Keep *generation*, *SQLite processing*, *Kafka publishing*, *Spark checkpointed Parquet ingestion*, and *PostgreSQL load* separate. An end-to-end latency distribution (p50/p95/p99) requires event-level timestamps measured at both ends; don't derive it from batch runtime.

Planned stages:
1. Establish a safe 100,000-event macOS baseline for deterministic JSONL generation and local SQLite processing.
2. Add isolated Kafka + Spark runs with explicit source offsets, partition counts, elapsed time and unique event reconciliation.
3. Measure processing lag/latency, resource use and checkpoint size; only then consider 1M and 5M synthetic events, subject to available storage and system memory.

## Stage 1 — 100,000-event local baseline

Run from repository root with your existing Python environment. This uses **new output paths** and does not modify your Kafka topics, existing Spark checkpoints or PostgreSQL databases.

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
python -m unittest discover -s tests -v
python -m streamshield.cli benchmark \
  --count 100000 \
  --output outputs/phase6_baseline_100k.jsonl \
  --database outputs/phase6_baseline_100k.db
```

**Important:** This exact command is for a **fresh trial** only; it will reuse the named database on a subsequent run, making the second result mostly duplicates and invalidating a first-ingest throughput comparison. For repeated trials, use distinct fresh file/database names (e.g. `phase6_baseline_100k_trial2`). Do not delete old results without reviewing them.

The CLI prints JSON including:
- `requested_events`, `generation_seconds`, `processing_seconds`
- `processing_events_per_second`
- `read`, `inserted`, `duplicate`, `rejected`, `alerts`
- `local_storage: "SQLite"`, `is_kafka_or_spark_benchmark: false`

Expected correctness: 100,000 records read and inserted on a fresh DB; 0 duplicates/rejected. Actual performance and alert counts must be taken from the output, not guessed.

Record Mac details to contextualize measurements:

```bash
sw_vers
uname -m
python --version
```

**Do not describe these results as Kafka/Spark throughput or real-time end-to-end latency.** This baseline excludes containerized streaming and network delivery.

## Observed macOS 100K results (November 25, 2026)

Environment: macOS 26.6.2 build 25G83, Apple Silicon (`arm64`), Python 3.11.9.

| Field | Fresh ingestion trial | Replay on existing DB |
|---|---:|---:|
| Requested/read | 100,000 / 100,000 | 100,000 / 100,000 |
| Generation seconds | 0.435 | 0.432 |
| Processing seconds | **0.501** | **0.474** |
| Processing events/sec | **199,494.0** | **211,121.9** |
| Inserted | 100,000 | 0 |
| Duplicates | 0 | 100,000 |
| Rejected | 0 | 0 |
| Newly created alerts | 1,608 | 0 |

The initial run reused `outputs/phase6_baseline_100k.db` and measured replay/duplicate handling; it is **not** a fresh-ingestion benchmark. The second run used the new `outputs/phase6_fresh_100k_trial2.db` and measured fresh insertion. Both are local JSONL-to-SQLite tests, exclude Kafka/Spark and exclude synthetic generation from the reported processing throughput. No streaming latency percentile is available.

## Stage 2 — Kafka -> Spark -> Parquet 100K benchmark (implementation ready; local results pending)

This is a **separate** six-partition Kafka input topic `streamshield.benchmark_100k.v1`. It generates 100,000 deterministic synthetic transactions starting from event index 500000 and uses `outputs/spark_benchmark_100k/` for fresh Spark checkpoints and Parquet. It does **not** modify the main 10,100-event dataset, the main topic, previous test topics, or PostgreSQL. The fixture refuses to republish into a nonempty topic, and the Spark benchmark refuses to overwrite existing benchmark outputs. Do not manually delete/checkpoint-reset to make a rerun appear fresh.

**Step A — Prerequisites and tests:**

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
python -m unittest discover -s tests -v
docker compose ps
```

Kafka must be healthy. If tests fail, stop.

**Step B — Publish exactly 100K records once:**

```bash
python -m streamshield.stream_bench_fixture
```

Expected (not guaranteed): `attempted=100000`, `acknowledged=100000`, `failed=0`, `unflushed=0`, `partitions=6`. Record producer elapsed seconds and acknowledged records/sec. This timing includes JSON serialization, publishing and delivery confirmation but excludes Kafka topic creation/preflight. Do not rerun the fixture after a successful publish.

**Step C — Process the source through the real Spark pipeline's validation, 10-minute watermark, event-ID deduplication and risk rules, writing checkpointed Parquet:**

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/stream_bench_spark.py
```

The resulting `spark_query_elapsed_seconds` measures **query startup, availableNow micro-batch processing and checkpointed Parquet completion**. It excludes SparkSession startup, Kafka publishing and the post-run aggregation. The `persisted_events_per_query_second` is the aggregate processing rate for that scope, **not** end-to-end throughput or per-event p95 latency.

If Spark fails or the count differs from 100K, do **not** delete output/checkpoints or rerun the fresh benchmark blindly: inspect log output first.

**Step D — Independent reconciliation:**

```bash
python -m streamshield.stream_bench_verify
```

Expected: `status=PASS`, `total_events=100000`, `unique_event_ids=100000`, `invalid_event_ids=0`, `reconciled_kafka_offsets=100000`. The verifier checks the six Kafka partition offset ranges are contiguous from zero and that all expected event IDs are present exactly once. It reads only the isolated Parquet output.

**Operational notes:** Spark may emit innocuous `NativeCodeLoader` or hostname binding warnings, as in earlier tests. However, an `ERROR`, failed query, unexpected counts or `FAIL` from the verifier require investigation, not a fabricated success. Memory, CPU, p50/p95/p99 latency and lag are not measured by these initial scripts. Publish results only after receiving the actual macOS console JSON.

**Repeat trials:** Use a distinct new topic name and distinct output directory in the code for each fresh run; the published v1 benchmark is intentionally one-shot to prevent accidental dataset reuse and corrupted throughput claims.

## Verified Phase 6B Kafka → Spark 100K reconciliation (November 25, 2026)

The independent read-only `stream_bench_verify` on the user's Apple Silicon Mac returned **PASS**: 100,000 persisted events; 100,000 unique event IDs; 0 invalid IDs; 100,000 reconciled Kafka offsets; no verifier errors. The six source partitions had counts **16,807 / 16,601 / 16,530 / 17,172 / 16,509 / 16,381**, each with contiguous offsets from 0 through `rows - 1` and no duplicate coordinates.

**Performance measurement pending:** The user has not supplied the separate Spark benchmark JSON containing `spark_query_elapsed_seconds` and `persisted_events_per_query_second`. Do not infer Kafka/Spark throughput or event latency from the read-only reconciliation results.

## Stage 3 — Fresh isolated v2 Kafka/Spark trial with saved timing (pending local run)

The original v1 100,000-event Spark output passed full six-partition reconciliation, but the console timing from the first Spark run was not saved. A later attempted v1 rerun was correctly blocked because its Parquet output already existed. The original timing **cannot be reconstructed accurately from the final Parquet row count**.

A separate **v2 trial** uses `streamshield.benchmark_100k.v2` and `outputs/spark_benchmark_100k_v2/`. The original v1 topic, outputs and checkpoints are not modified. The v2 Spark driver saves the measured benchmark JSON to `outputs/spark_benchmark_100k_v2/benchmark_result.json` after completing and validating the query. The publisher refuses to use a nonempty topic, and the Spark benchmark refuses to reuse existing checkpoints/outputs. This is a **new** trial, not recovery of v1 elapsed time.

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
python -m unittest discover -s tests -v
docker compose ps
```

First publish the v2 Kafka fixture **once**:

```bash
python -m streamshield.stream_bench_fixture --trial v2
```

Expect 100,000 acknowledgements and 0 delivery failures. After confirming that result, run Spark with the dedicated output:

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  src/streamshield/stream_bench_spark.py --trial v2
```

**To display the saved performance result**:

```bash
cat outputs/spark_benchmark_100k_v2/benchmark_result.json
```

Independently verify six partition offset ranges and event IDs:

```bash
python -m streamshield.stream_bench_verify --trial v2
```

Do not run the publisher twice or erase existing outputs. If the Spark job fails partway through, **do not rerun the fresh benchmark without inspection**. Report the error first. The `spark_query_elapsed_seconds` is the Spark query's aggregate runtime and does not provide per-event p95/p99 latency or separate producer-to-consumer wall-clock timing.

**Verified v2 Kafka producer result (November 25, 2026):** Topic `streamshield.benchmark_100k.v2`; 6 partitions; 100,000 attempted/acknowledged, 0 failed, 0 unflushed; measured producer duration **0.987 seconds** and acknowledged rate **101,282.7 events/sec**. This is publisher-side timing and does not measure Spark processing or end-to-end latency. The v2 Spark trial and independent reconciliation passed; see the verified timing section.

## Publishing guidelines

Report OS, Python version, available resources, event count, reproducibility seed, command, raw output, timestamp, warm-up policy, and whether the measured duration includes fixture creation. Don't compare dissimilar measurement scopes as if they represent the same throughput.

**Verified v2 Spark data reconciliation (November 25, 2026):** Read-only verifier returned `PASS`, `total_events=100000`, `unique_event_ids=100000`, `invalid_event_ids=0`, and `reconciled_kafka_offsets=100000` with no errors. All six Kafka partitions had contiguous offset ranges and per-partition counts 16,807 / 16,601 / 16,530 / 17,172 / 16,509 / 16,381. The saved v2 Spark processing-time JSON has now been supplied and its values appear in the verified timing section below.


## Verified Phase 6C v2 100K streaming benchmark timing (November 25, 2026)

On macOS 26.6.2, Apple Silicon arm64, Python 3.11.9, the user captured the saved `outputs/spark_benchmark_100k_v2/benchmark_result.json`:

| Measurement | Result |
|---|---:|
| Source Kafka topic | `streamshield.benchmark_100k.v2` |
| Source/accepted events | 100,000 / 100,000 |
| Distinct accepted event IDs | 100,000 |
| Kafka offsets reconciled by independent verifier | 100,000 (six partitions) |
| Synthetic illustrative risk flags | 1,609 |
| Kafka producer acknowledgements | 100,000 (0 failed, 0 unflushed) |
| Kafka producer elapsed | **0.987 s** |
| Kafka acknowledged producer rate | **101,282.7 events/s** |
| Spark availableNow query elapsed | **8.788 s** |
| Spark persisted rows/query second | **11,379.5 events/s** |
| Independent reconciliation | **PASS** |

Spark query timing includes the call to start the query, `availableNow` processing and checkpointed Parquet sink completion. It excludes Kafka producer time, SparkSession initialization, and post-run verification. Kafka producer and Spark were benchmarked sequentially as distinct stages; do not treat the rate comparison as an end-to-end simultaneous pipeline throughput measurement or p95 latency. This is a single local test; stability and larger-scale performance remain to be measured.

