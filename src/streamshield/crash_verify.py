"""Independent read-only reconciliation of the isolated Spark forced-crash test."""
from __future__ import annotations
import argparse
import json
from .crash_fixture import COUNT, START


def verify(records: list[tuple[int, int, str]], full: bool = False) -> dict:
    offsets = [(int(partition), int(offset)) for partition, offset, _ in records]
    event_ids = [event_id for _, _, event_id in records]
    if len(set(offsets)) != len(offsets):
        raise AssertionError("duplicate Kafka partition/offset coordinates persisted")
    if len(set(event_ids)) != len(event_ids):
        raise AssertionError("duplicate event_id persisted")
    expected = {f"evt_{i:012d}" for i in range(START, START + COUNT)}
    if not set(event_ids).issubset(expected):
        raise AssertionError("unexpected IDs in crash-test Parquet")
    if full:
        if len(records) != COUNT or set(event_ids) != expected:
            raise AssertionError(f"Expected exactly {COUNT} unique IDs, got {len(records)}")
        if set(offsets) != {(0, i) for i in range(COUNT)}:
            raise AssertionError("Kafka offsets 0..299 not fully accounted for")
    else:
        if not 0 < len(records) < COUNT:
            raise AssertionError(f"Expected partial progress (1..{COUNT - 1}), got {len(records)}")
    return {"status": "PASS", "verification": "full" if full else "partial",
            "persisted_events": len(records), "unique_event_ids": len(set(event_ids)),
            "unique_kafka_offsets": len(set(offsets))}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--full", action="store_true")
    p.add_argument("--path", default="outputs/spark_crash_phase5/accepted")
    args = p.parse_args()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[2]").appName("StreamShieldCrashVerify").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        records = [(int(r["kafka_partition"]), int(r["kafka_offset"]), r["event_id"])
                   for r in spark.read.parquet(args.path)
                   .select("kafka_partition", "kafka_offset", "event_id").collect()]
        print(json.dumps(verify(records, args.full), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
