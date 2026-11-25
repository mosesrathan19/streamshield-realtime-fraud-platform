"""Independent read-only 1M benchmark reconciliation of Parquet IDs and Kafka offsets."""
from __future__ import annotations
import argparse
import json
from streamshield.scale_1m_fixture import COUNT, START, PARTITIONS, OUTPUT


def assess(total: int, distinct: int, invalid: int, groups: list[dict]) -> dict:
    errors = []
    if total != COUNT:
        errors.append(f"Expected {COUNT} persisted records; found {total}")
    if distinct != COUNT:
        errors.append(f"Expected {COUNT} distinct event IDs; found {distinct}")
    if invalid:
        errors.append(f"{invalid} event IDs outside expected fixture range/format")
    if len(groups) != PARTITIONS or sorted(g["partition"] for g in groups) != list(range(PARTITIONS)):
        errors.append("Missing or unexpected Kafka partition in persisted records")
    for g in groups:
        if g["min_offset"] != 0 or g["max_offset"] != g["rows"] - 1:
            errors.append(f"Kafka partition {g['partition']} offsets not contiguous from zero")
        if g["distinct_offsets"] != g["rows"]:
            errors.append(f"Kafka partition {g['partition']} contains duplicate offsets")
    if sum(g["rows"] for g in groups) != total:
        errors.append("Partition row counts do not sum to total persisted rows")
    return {
        "status": "FAIL" if errors else "PASS",
        "total_events": total,
        "unique_event_ids": distinct,
        "invalid_event_ids": invalid,
        "reconciled_kafka_offsets": sum(g["distinct_offsets"] for g in groups),
        "partitions": groups,
        "errors": errors
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default=str(OUTPUT / "accepted"))
    args = parser.parse_args()
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.master("local[2]")
             .appName("StreamShieldOneMillionVerifier").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        data = spark.read.parquet(args.path)
        extracted = F.regexp_extract(F.col("event_id"), r"^evt_(\d{12})$", 1)
        id_value = F.when(extracted != "", extracted.cast("long"))
        with_ids = data.withColumn("expected_index", id_value)
        aggregate = with_ids.agg(
            F.count("*").alias("total"),
            F.countDistinct("event_id").alias("distinct"),
            F.sum(F.when(F.col("expected_index").isNull()
                         | (F.col("expected_index") < START)
                         | (F.col("expected_index") >= START + COUNT), 1)
                  .otherwise(0)).alias("invalid")
        ).first()
        groups = [
            {
                "partition": int(r["kafka_partition"]),
                "rows": int(r["rows"]),
                "min_offset": int(r["min_offset"]),
                "max_offset": int(r["max_offset"]),
                "distinct_offsets": int(r["distinct_offsets"]),
            }
            for r in data.groupBy("kafka_partition").agg(
                F.count("*").alias("rows"),
                F.min("kafka_offset").alias("min_offset"),
                F.max("kafka_offset").alias("max_offset"),
                F.countDistinct("kafka_offset").alias("distinct_offsets")
            ).collect()
        ]
        groups.sort(key=lambda g: g["partition"])
        result = assess(int(aggregate["total"]), int(aggregate["distinct"]),
                        int(aggregate["invalid"] or 0), groups)
        print(json.dumps(result, indent=2))
        if result["status"] != "PASS":
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
