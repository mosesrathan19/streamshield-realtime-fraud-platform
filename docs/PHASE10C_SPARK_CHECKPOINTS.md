# Phase 10C — Spark Structured Streaming Checkpoint Progress

**Status:** Locally verified on macOS November 25, 2026 (dashboard and unit tests). The Streamlit Spark Checkpoints page displayed three existing checkpoints: Phase 3 accepted (4 offset logs, 4 commit logs, 4 committed batch IDs), 100K v2 accepted (2/2/2), and 1M v1 accepted (2/2/2), for 8 committed batch IDs total. The user subsequently confirmed the Phase 10C unit tests passed on November 25, 2026; exact test output/count was not provided.

## What is implemented

The Streamlit dashboard now has a **Spark Checkpoints** page. It reads the existing Spark checkpoint `offsets/` and `commits/` directories **read-only**, for:

- Phase 3 accepted stream — `outputs/spark_phase3/checkpoints/accepted`
- Phase 6 100K v2 accepted stream — `outputs/spark_benchmark_100k_v2/checkpoints/accepted`
- Phase 6 1M v1 accepted stream — `outputs/spark_benchmark_1m_v1/checkpoints/accepted`

A batch is shown only if **both** a numeric offset-log file and a matching numeric commit-log file exist. The page reports committed batch IDs and source end offsets; where two consecutive visible batches use the same topic/partitions with nondecreasing offsets, the difference is shown as **source offset movement**, not accepted transaction counts.

The helper only reads small text files, skips `.crc` and metadata files, caps each file to 2 MB and the displayed history to 1,000 batches per checkpoint. It **does not** call Spark, connect to Kafka, restart jobs, publish events, or alter checkpoints. The Docker dashboard already mounts `./outputs:/app/outputs:ro`.

## Accurate interpretation

**This is historical saved checkpoint information, NOT proof of a running Spark stream.** Kafka end offsets saved in Spark checkpoint logs are not equal to final Parquet row counts, especially with validation, watermarks and event-ID deduplication.

The old runs saved a completed-query duration and aggregate rate in separate benchmark JSON files, but **did not save per-microbatch `StreamingQueryProgress`** fields such as `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, and `durationMs`.

**Phase 10C does not invent these measurements.** To report them for future runs, the Spark scripts would need to write an explicit progress report or listener log during a **new, separate fixture run**, leaving existing successful benchmarks and their checkpoints intact.

## Verify on Mac

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
python -m unittest discover -s tests -v
docker compose --profile dashboard up -d --build
```

Open **http://localhost:8501**, then select **Spark Checkpoints** from the sidebar.

Expected: the three saved checkpoint directories appear as present, with offset/commit log file counts and committed batch summaries. The number of batches depends on the historical runs; no number is predeclared.

If a checkpoint is absent, the page reports `Missing` instead of attempting any recovery or recreation.

**Safe shutdown (dashboard only):**

```bash
docker compose --profile dashboard stop dashboard
```

Avoid deleting any volume, Spark checkpoint, topic or Parquet output. This is a historical monitoring view, not an end-to-end exactly-once guarantee.

## Tests

`tests/test_spark_checkpoints.py` adds five offline cases for missing directories, committed versus uncommitted batches, monotonic offset movement, invalid metadata and non-log-file filtering. These need local execution before marking this phase verified.
