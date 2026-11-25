"""Run the isolated Phase 11 3K Kafka -> Spark -> Parquet microbatch fixture.

Never reuses previous benchmark topics, outputs or checkpoints. Uses the
exact same enrichment path with an opt-in maxOffsetsPerTrigger=500 limit.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from streamshield.microbatch_fixture import TOPIC, COUNT
from streamshield.microbatch_metrics import progress_report
from streamshield.spark_stream import enriched_stream, SPARK_VERSION

OUTPUT = Path("outputs/spark_microbatch_3k_v1")
MAX_OFFSETS_PER_TRIGGER = 500


def preflight(root: Path) -> None:
    for name in ("accepted", "checkpoints", "progress_report.json", "run_result.json"):
        if (root / name).exists():
            raise RuntimeError(
                f"Existing Phase 11 output: {root / name}. "
                "Refusing to overwrite or reuse. Inspect the existing run instead."
            )


def run(brokers: str = "127.0.0.1:9092") -> dict:
    from pyspark.sql import SparkSession, functions as F

    root = OUTPUT.resolve()
    preflight(root)
    spark = (SparkSession.builder.master("local[2]")
             .appName("StreamShieldMicrobatchProgressFixture")
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    try:
        if spark.version != SPARK_VERSION:
            raise RuntimeError(f"Expected Spark {SPARK_VERSION}, found {spark.version}")
        spark.sparkContext.setLogLevel("WARN")

        df = enriched_stream(
            spark, brokers, "10 minutes", TOPIC,
            max_offsets_per_trigger=MAX_OFFSETS_PER_TRIGGER,
        )
        query = (df.writeStream.format("parquet")
                 .outputMode("append")
                 .option("path", str(root / "accepted"))
                 .option("checkpointLocation", str(root / "checkpoints" / "accepted"))
                 .trigger(availableNow=True)
                 .start())
        query.awaitTermination()
        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))

        # recentProgress is captured from THIS query, not historical checkpoints.
        events = list(query.recentProgress or [])
        progress = progress_report(events, topic=TOPIC)
        rows = spark.read.parquet(str(root / "accepted"))
        summary = rows.agg(
            F.count("*").alias("total"),
            F.countDistinct("event_id").alias("unique_ids"),
        ).first()
        persisted, distinct = int(summary["total"]), int(summary["unique_ids"])
        if persisted != COUNT or distinct != COUNT:
            raise RuntimeError(
                f"Incomplete or nonunique fixture: persisted={persisted}, "
                f"unique={distinct}; preserving outputs for diagnosis."
            )
        if progress["total_input_rows_across_batches"] != COUNT:
            raise RuntimeError(
                "Spark query progress did not account for all fixture inputs; "
                "preserving outputs and refusing to publish a complete report."
            )
        if progress["progress_rows"] < 2:
            raise RuntimeError(
                "Expected multiple Spark progress batches. No result report saved."
            )

        result = {
            "status": "PASS",
            "source_topic": TOPIC,
            "expected_input_events": COUNT,
            "persisted_events": persisted,
            "distinct_event_ids": distinct,
            "recorded_microbatches": progress["progress_rows"],
            "max_offsets_per_trigger": MAX_OFFSETS_PER_TRIGGER,
            "output_dir": str(root),
        }
        root.mkdir(parents=True, exist_ok=True)
        with (root / "progress_report.json").open("x", encoding="utf-8") as fh:
            json.dump(progress, fh, indent=2)
            fh.write("\n")
        with (root / "run_result.json").open("x", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2)
            fh.write("\n")
        print(json.dumps(result, indent=2), flush=True)
        return result
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="127.0.0.1:9092")
    args = parser.parse_args()
    run(args.brokers)


if __name__ == "__main__":
    main()
