"""Read-only independent verification of the isolated Spark DLQ Parquet sinks."""
from __future__ import annotations
import argparse
import json
from collections import Counter

EXPECTED_ACCEPTED = {"dlq_good_1", "dlq_good_2"}
EXPECTED_REJECTED = {
    ("dlq_bad_amount", "invalid_amount"),
    ("dlq_bad_account", "missing_account_id"),
    ("dlq_bad_time", "invalid_event_time"),
    (None, "missing_or_malformed_event_id"),
}


def verify(accepted: list[dict], rejected: list[dict]) -> dict:
    aid = [x["event_id"] for x in accepted]
    rejects = {(x["event_id"], x["reject_reason"]) for x in rejected}
    offsets = [(x["source_topic"], x["source_partition"], x["source_offset"])
               for x in accepted + rejected]
    if len(accepted) != 2 or set(aid) != EXPECTED_ACCEPTED or len(set(aid)) != 2:
        raise AssertionError(f"accepted mismatch: {aid}")
    if len(rejected) != 4 or rejects != EXPECTED_REJECTED:
        raise AssertionError(f"rejected mismatch: {sorted(str(x) for x in rejects)}")
    if len(set(offsets)) != 6:
        raise AssertionError("duplicate Kafka coordinates across accepted/rejected sinks")
    if sorted(x[2] for x in offsets) != list(range(6)):
        raise AssertionError("not all six Kafka offsets accounted for")
    return {"status": "PASS", "total": 6, "accepted": 2, "rejected": 4,
            "reasons": dict(sorted(Counter(x["reject_reason"] for x in rejected).items())),
            "reconciled_kafka_offsets": 6}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default="outputs/spark_dlq_phase5")
    args = p.parse_args()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[2]").appName("StreamShieldDLQVerify").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        accepted = [r.asDict() for r in spark.read.parquet(
            args.output_dir + "/accepted").select(
            "source_topic", "source_partition", "source_offset", "event_id").collect()]
        rejected = [r.asDict() for r in spark.read.parquet(
            args.output_dir + "/rejected").select(
            "source_topic", "source_partition", "source_offset",
            "event_id", "reject_reason").collect()]
        print(json.dumps(verify(accepted, rejected), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
