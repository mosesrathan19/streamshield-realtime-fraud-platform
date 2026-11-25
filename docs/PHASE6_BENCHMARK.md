# Phase 6 — Reproducible performance benchmarking

**Status:** Phase 6 plan and local baseline instructions committed. No Phase 6 results have yet been measured on the user's Mac.

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

## Stage 2 — Streaming benchmark (pending implementation)

Will use an isolated Kafka topic and fresh Spark checkpoint/output, separate from the verified main 10,100-row dataset. We'll measure Kafka producer acknowledgement rate, Spark ingestion/reconciliation, and system resources. A total-run-time measurement is not equivalent to per-event p95 latency. Progressively increase counts after the small test is verified.

## Publishing guidelines

Report OS, Python version, available resources, event count, reproducibility seed, command, raw output, timestamp, warm-up policy, and whether the measured duration includes fixture creation. Don't compare dissimilar measurement scopes as if they represent the same throughput.
