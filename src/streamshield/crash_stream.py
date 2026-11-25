"""Phase 5E: intentionally SIGKILL an isolated Spark streaming driver, then recover.

Run only with dedicated fixture topic and isolated output. Normal recovery uses
the same checkpoint and transactional Parquet file sink (not a foreachBatch sink).
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import time
from pathlib import Path

from streamshield.crash_fixture import TOPIC, COUNT
from streamshield.spark_stream import SPARK_VERSION, schema

OUTPUT = "outputs/spark_crash_phase5"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["crash", "recover"], required=True)
    p.add_argument("--brokers", default="127.0.0.1:9092")
    p.add_argument("--confirm-isolated-crash", action="store_true",
                   help="required for the SIGKILL test")
    args = p.parse_args()
    if args.mode == "crash" and not args.confirm_isolated_crash:
        p.error("crash mode requires --confirm-isolated-crash")
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.master("local[2]")
             .appName("StreamShieldIsolatedForcedCrash")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    if spark.version != SPARK_VERSION:
        raise RuntimeError(f"Spark {SPARK_VERSION} required, found {spark.version}")
    spark.sparkContext.setLogLevel("WARN")
    folder = Path(OUTPUT).resolve()
    folder.mkdir(parents=True, exist_ok=True)

    # Never point this script at the primary topic or any existing Spark output.
    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.brokers)
              .option("subscribe", TOPIC)
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", 50)
              .option("failOnDataLoss", "true")
              .load())
    persisted = (source.select(
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.from_json(F.col("value").cast("string"), schema()).alias("payload")
    ).select("kafka_partition", "kafka_offset", F.col("payload.event_id").alias("event_id")))
    writer = (persisted.writeStream.format("parquet").outputMode("append")
              .option("path", str(folder / "accepted"))
              .option("checkpointLocation", str(folder / "checkpoints" / "accepted"))
              .option("maxRecordsPerFile", "10000"))
    # Limit Kafka consumption per trigger so the crash occurs before all 300
    # source events have been processed, enabling an observable partial run.
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    if args.mode == "crash":
        query = writer.trigger(processingTime="2 seconds").start()
        try:
            seen_progress = -1
            while query.isActive:
                progress = query.lastProgress
                if progress is not None:
                    batch = int(progress["batchId"])
                    if batch != seen_progress:
                        seen_progress = batch
                        print(json.dumps({
                            "last_completed_batch": batch,
                            "last_batch_input_rows": progress["numInputRows"],
                            "message": "Spark reported a completed micro-batch"
                        }), flush=True)
                    if batch >= 1 and int(progress["numInputRows"]) > 0:
                        print("EXPECTED FORCED SPARK DRIVER TERMINATION: SIGKILL",
                              flush=True)
                        os.kill(os.getpid(), signal.SIGKILL)
                time.sleep(0.3)
            raise RuntimeError("Spark stopped before forced-crash condition occurred")
        finally:
            # SIGKILL bypasses finally on purpose.
            spark.stop()
    else:
        query = writer.trigger(availableNow=True).start()
        try:
            query.awaitTermination()
            if query.exception() is not None:
                raise RuntimeError(str(query.exception()))
            data = spark.read.parquet(str(folder / "accepted"))
            agg = data.agg(F.count("*").alias("rows"),
                           F.countDistinct("event_id").alias("unique")).first()
            print(json.dumps({
                "mode": "recover",
                "persisted_events": agg["rows"],
                "unique_ids": agg["unique"],
                "expected_count": COUNT
            }, indent=2))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
