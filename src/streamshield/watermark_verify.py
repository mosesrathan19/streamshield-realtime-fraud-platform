"""Validate isolated Spark watermark fixture output without mutating it."""
from __future__ import annotations
import argparse
import json

SEED_IDS = {"wm_seed_1", "wm_seed_2"}
ACCEPTED_IDS = SEED_IDS | {"wm_outorder", "wm_on_time"}
LATE_ID = "wm_too_late"


def verify_ids(ids: list[str], stage: str) -> dict:
    expected = SEED_IDS if stage == "seed" else ACCEPTED_IDS
    actual = set(ids)
    if len(ids) != len(actual):
        raise AssertionError(f"duplicate saved event IDs: {len(ids)} rows, {len(actual)} unique")
    if actual != expected:
        raise AssertionError(f"expected {sorted(expected)}, observed {sorted(actual)}. "
                             "Watermark timing may differ by micro-batch; inspect progress.")
    return {"status": "PASS", "stage": stage, "persisted_events": len(ids),
            "unique_event_ids": len(actual), "event_ids": sorted(actual),
            "late_event_absent": LATE_ID not in actual}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("stage", choices=("seed", "followup"))
    p.add_argument("--path", default="outputs/spark_watermark_phase5/accepted")
    a = p.parse_args()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[2]").appName("StreamShieldWatermarkVerify").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        records = [r["event_id"] for r in spark.read.parquet(a.path).select("event_id").collect()]
        print(json.dumps(verify_ids(records, a.stage), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
