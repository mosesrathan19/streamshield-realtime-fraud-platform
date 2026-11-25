"""Read-only reconciliation for Phase 6 isolated Kafka -> Spark benchmark."""
from __future__ import annotations
import argparse
import json
from .stream_bench_fixture import COUNT, START, PARTITIONS

OUTPUT = "outputs/spark_benchmark_100k/accepted"


def assess(total: int, distinct_ids: int, invalid_ids: int,
           partitions: list[dict]) -> dict:
    problems = []
    if total != COUNT:
        problems.append(f"expected {COUNT} rows, observed {total}")
    if distinct_ids != COUNT:
        problems.append(f"expected {COUNT} unique event IDs, observed {distinct_ids}")
    if invalid_ids:
        problems.append(f"{invalid_ids} event IDs outside the expected 100K range")
    if len(partitions) != PARTITIONS:
        problems.append(f"expected {PARTITIONS} Kafka partitions, saw {len(partitions)}")
    total_offsets = 0
    for item in partitions:
        p = item["partition"]
        n = item["rows"]
        total_offsets += n
        if not (0 <= p < PARTITIONS):
            problems.append(f"unexpected partition {p}")
        if item["min_offset"] != 0 or item["max_offset"] != n - 1:
            problems.append(f"partition {p} offset range is not contiguous from zero")
        if item["distinct_offsets"] != n:
            problems.append(f"partition {p} contains duplicate offsets")
    if total_offsets != total:
        problems.append("partition event-count sum differs from persisted rows")
    return {
        "status": "PASS" if not problems else "FAIL",
        "total_events": total,
        "unique_event_ids": distinct_ids,
        "invalid_event_ids": invalid_ids,
        "reconciled_kafka_offsets": total_offsets,
        "partitions": partitions,
        "errors": problems,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default=OUTPUT)
    args = parser.parse_args()
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.master("local[2]")
             .appName("StreamShieldPhase6BenchmarkVerifier").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.read.parquet(args.path)
        idx = F.regexp_extract(F.col("event_id"), r"^evt_(\d{12})$", 1)
        checked = df.withColumn("fixture_index", F.when(idx != "", idx.cast("long")))
        metrics = checked.agg(
            F.count("*").alias("rows"),
            F.countDistinct("event_id").alias("unique"),
            F.sum(F.when(F.col("fixture_index").isNull()
                         | (F.col("fixture_index") < START)
                         | (F.col("fixture_index") >= START + COUNT), 1)
                  .otherwise(0)).alias("bad_ids"),
        ).first()
        offsets = [dict(
            partition=int(row["kafka_partition"]),
            rows=int(row["rows"]),
            min_offset=int(row["min_offset"]),
            max_offset=int(row["max_offset"]),
            distinct_offsets=int(row["distinct_offsets"]),
        ) for row in df.groupBy("kafka_partition").agg(
            F.count("*").alias("rows"),
            F.min("kafka_offset").alias("min_offset"),
            F.max("kafka_offset").alias("max_offset"),
            F.countDistinct("kafka_offset").alias("distinct_offsets"),
        ).collect()]
        offsets.sort(key=lambda x: x["partition"])
        result = assess(int(metrics["rows"]), int(metrics["unique"]),
                        int(metrics["bad_ids"] or 0), offsets)
        print(json.dumps(result, indent=2))
        if result["status"] != "PASS":
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
