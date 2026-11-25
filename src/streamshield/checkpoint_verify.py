"""Read-only Phase 5 assertion for the persisted Spark checkpointed Parquet sink.

Usage:
 python -m streamshield.checkpoint_verify --expected-count 10000
Never changes checkpoint files, Kafka, or Parquet data.
"""
from __future__ import annotations
import argparse
import json


def check_metrics(actual_count: int, distinct_count: int, expected_count: int) -> dict:
    if expected_count <= 0:
        raise ValueError("expected-count must be positive")
    if actual_count != expected_count:
        raise AssertionError(
            f"expected {expected_count} persisted records; observed {actual_count}"
        )
    if distinct_count != actual_count:
        raise AssertionError(
            f"duplicate event IDs detected: rows={actual_count}, distinct={distinct_count}"
        )
    return {"status": "PASS", "persisted_events": actual_count,
            "unique_event_ids": distinct_count, "expected_events": expected_count}


def main():
    parser = argparse.ArgumentParser(description="Verify Spark Parquet count and unique IDs")
    parser.add_argument("--path", default="outputs/spark_phase3/accepted")
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--check-new-range", action="store_true",
                        help="verify all event indices 10000 through 10099 are present")
    args = parser.parse_args()

    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.master("local[2]")
             .appName("StreamShieldCheckpointVerify")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.read.parquet(args.path)
        agg = df.agg(F.count("*").alias("rows"),
                     F.countDistinct("event_id").alias("unique"),
                     F.sum(F.col("risk_flag").cast("long")).alias("flagged")).first()
        result = check_metrics(agg["rows"], agg["unique"], args.expected_count)
        result["synthetic_risk_flags"] = agg["flagged"]
        if args.check_new_range:
            new_ids = [f"evt_{i:012d}" for i in range(10000, 10100)]
            observed = {row["event_id"] for row in
                        df.filter(F.col("event_id").isin(new_ids)).select("event_id").collect()}
            if len(observed) != 100:
                raise AssertionError(f"missing {100 - len(observed)} IDs in new 100-event range")
            result["new_event_ids_verified"] = 100
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
