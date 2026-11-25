"""Phase 6D: 1M Kafka -> existing Spark validation/dedup/rules -> Parquet.

Measured query interval is start() through availableNow awaitTermination(),
excluding SparkSession initialization, publisher, and output verification.
Results are written to a fresh, exclusive JSON file.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from time import perf_counter

from streamshield.spark_stream import SPARK_VERSION, enriched_stream
from streamshield.scale_1m_fixture import TOPIC, COUNT, OUTPUT, preflight_space


def run(brokers: str = "127.0.0.1:9092") -> dict:
    space = preflight_space()
    if not space["sufficient"]:
        raise RuntimeError("Insufficient host disk space for a 1M Spark benchmark")
    folder = OUTPUT.resolve()
    for child in ("accepted", "checkpoints", "benchmark_result.json"):
        if (folder / child).exists():
            raise RuntimeError(f"Existing benchmark data: {folder / child}; "
                               "refusing to remeasure partial/resumed work as a fresh trial")
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.master("local[2]")
             .appName("StreamShieldPhase6DOneMillion")
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    if spark.version != SPARK_VERSION:
        raise RuntimeError(f"Expected Spark {SPARK_VERSION}; found {spark.version}")
    spark.sparkContext.setLogLevel("WARN")
    try:
        accepted = enriched_stream(spark, brokers, "10 minutes", TOPIC)
        begin = perf_counter()
        query = (accepted.writeStream.format("parquet")
                 .outputMode("append")
                 .option("path", str(folder / "accepted"))
                 .option("checkpointLocation", str(folder / "checkpoints" / "accepted"))
                 .trigger(availableNow=True).start())
        query.awaitTermination()
        elapsed = perf_counter() - begin
        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))
        df = spark.read.parquet(str(folder / "accepted"))
        row = df.agg(
            F.count("*").alias("rows"),
            F.countDistinct("event_id").alias("unique"),
            F.sum(F.col("risk_flag").cast("long")).alias("flags")
        ).first()
        total, unique = int(row["rows"]), int(row["unique"])
        metrics = {
            "source_topic": TOPIC,
            "expected_input_events": COUNT,
            "persisted_events": total,
            "distinct_event_ids": unique,
            "synthetic_risk_flags": int(row["flags"] or 0),
            "spark_query_elapsed_seconds": round(elapsed, 3),
            "persisted_events_per_query_second": round(total / elapsed, 1),
            "query_scope": "Spark start + availableNow processing + checkpointed Parquet completion",
            "excludes": ["producer publishing", "SparkSession startup", "post-run verification"],
            "output_dir": str(folder)
        }
        print(json.dumps(metrics, indent=2), flush=True)
        if total != COUNT or unique != COUNT:
            raise RuntimeError("Incomplete 1M result; preserving output for troubleshooting")
        report = folder / "benchmark_result.json"
        with report.open("x", encoding="utf-8") as fh:
            json.dump(metrics, fh, indent=2)
            fh.write("\n")
        print(f"Saved Spark benchmark report: {report}", flush=True)
        return metrics
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="127.0.0.1:9092")
    args = parser.parse_args()
    run(args.brokers)


if __name__ == "__main__":
    main()
