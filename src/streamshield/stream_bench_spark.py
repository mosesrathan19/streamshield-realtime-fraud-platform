"""Phase 6 isolated Kafka -> Spark enriched-stream -> Parquet benchmark.

Elapsed processing time covers query.start() through awaitTermination() but
excludes Spark initialization, output verification, producer publishing,
event generation and Kafka topic creation. Reusing an old output is refused.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from time import perf_counter

from streamshield.spark_stream import SPARK_VERSION, enriched_stream
from streamshield.stream_bench_fixture import TOPIC, COUNT, START, PARTITIONS

OUTPUT = "outputs/spark_benchmark_100k"


def check_output_is_fresh(folder: Path):
    for name in ("accepted", "checkpoints"):
        if (folder / name).exists():
            raise RuntimeError(
                f"{folder / name} already exists; refusing to benchmark a resumed "
                "or partially completed run as fresh ingestion")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="127.0.0.1:9092")
    args = parser.parse_args()
    from pyspark.sql import SparkSession, functions as F
    folder = Path(OUTPUT).resolve()
    check_output_is_fresh(folder)
    spark = (SparkSession.builder.master("local[2]")
             .appName("StreamShieldPhase6KafkaSparkBenchmark")
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    if spark.version != SPARK_VERSION:
        raise RuntimeError(f"Expected Spark {SPARK_VERSION}; got {spark.version}")
    spark.sparkContext.setLogLevel("WARN")
    try:
        incoming = enriched_stream(spark, args.brokers, "10 minutes", TOPIC)
        # This is the same parsing, validation, event-time watermark, duplicate
        # suppression and illustrative risk-rule logic as the primary Spark path.
        query_start = perf_counter()
        query = (incoming.writeStream.format("parquet")
                 .outputMode("append")
                 .option("path", str(folder / "accepted"))
                 .option("checkpointLocation", str(folder / "checkpoints" / "accepted"))
                 .trigger(availableNow=True).start())
        query.awaitTermination()
        elapsed = perf_counter() - query_start
        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))
        # Read output only after query has stopped; exclude this from the timing.
        data = spark.read.parquet(str(folder / "accepted"))
        result = data.agg(
            F.count("*").alias("persisted_events"),
            F.countDistinct("event_id").alias("distinct_event_ids"),
            F.sum(F.col("risk_flag").cast("long")).alias("synthetic_risk_flags"),
        ).first()
        observed = int(result["persisted_events"])
        payload = {
            "source_topic": TOPIC,
            "expected_input_events": COUNT,
            "persisted_events": observed,
            "distinct_event_ids": int(result["distinct_event_ids"]),
            "synthetic_risk_flags": int(result["synthetic_risk_flags"] or 0),
            "spark_query_elapsed_seconds": round(elapsed, 3),
            "persisted_events_per_query_second": round(observed / elapsed, 1),
            "query_scope": "Spark start + availableNow processing + checkpointed Parquet completion",
            "excludes": ["producer publishing", "SparkSession startup", "post-run verification"],
            "output_dir": str(folder),
        }
        print(json.dumps(payload, indent=2))
        # Source offset accounting happens in a separate read-only verifier.
        if observed != COUNT or int(result["distinct_event_ids"]) != COUNT:
            raise RuntimeError("Persisted event count is not the expected 100K; "
                               "do not publish a PASS result; inspect watermark/source offsets")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
