"""Phase 5D: isolated Spark Kafka data-quality routing to accepted and rejected Parquet.

The rejected sink is a durable local DLQ *equivalent* for test purposes, not a
Kafka DLQ topic. It stores Kafka offset coordinates and reason, not raw payload.
Uses distinct checkpoints for accepted and rejected sinks.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from streamshield.spark_stream import SPARK_VERSION, schema

TOPIC = "streamshield.dlq_test.v1"


def classified_stream(spark, brokers: str, topic: str = TOPIC):
    from pyspark.sql import functions as F
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", brokers)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "true")
           .load())
    parsed = raw.select(
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.from_json(F.col("value").cast("string"), schema()).alias("payload"),
    ).select("source_topic", "source_partition", "source_offset", "payload.*")
    parsed = parsed.withColumn("event_ts", F.to_timestamp("event_time"))

    # Spark may map invalid JSON to a struct of nulls.
    reason = (F.when(F.col("event_id").isNull(), F.lit("missing_or_malformed_event_id"))
              .when(F.col("transaction_id").isNull(), F.lit("missing_transaction_id"))
              .when(F.col("account_id").isNull(), F.lit("missing_account_id"))
              .when(F.col("merchant_id").isNull(), F.lit("missing_merchant_id"))
              .when(F.col("amount_cents").isNull() | (F.col("amount_cents") <= 0),
                    F.lit("invalid_amount"))
              .when(F.col("currency") != "USD", F.lit("unsupported_currency"))
              .when(F.col("currency").isNull(), F.lit("missing_currency"))
              .when(F.col("event_type") != "payment_authorized", F.lit("unsupported_event_type"))
              .when(F.col("event_type").isNull(), F.lit("missing_event_type"))
              .when(F.col("schema_version") != 1, F.lit("unsupported_schema_version"))
              .when(F.col("schema_version").isNull(), F.lit("missing_schema_version"))
              .when(F.col("event_ts").isNull(), F.lit("invalid_event_time"))
              .otherwise(F.lit(None).cast("string")))
    return parsed.withColumn("reject_reason", reason)


def run(brokers: str, output_dir: str, topic: str = TOPIC) -> None:
    from pyspark.sql import SparkSession, functions as F
    if topic != TOPIC:
        raise ValueError("DLQ demonstration restricted to isolated fixture topic")
    spark = (SparkSession.builder.master("local[2]")
             .appName("StreamShieldIsolatedDLQ")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    if spark.version != SPARK_VERSION:
        raise RuntimeError(f"Expected Spark {SPARK_VERSION}; got {spark.version}")
    spark.sparkContext.setLogLevel("WARN")
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    try:
        classified = classified_stream(spark, brokers, topic)
        accepted = (classified.filter(F.col("reject_reason").isNull())
                    .select("source_topic", "source_partition", "source_offset",
                            "event_id", "account_id", "event_ts", "amount_cents"))
        rejected = (classified.filter(F.col("reject_reason").isNotNull())
                    .select("source_topic", "source_partition", "source_offset",
                            "event_id", "reject_reason"))
        # Independent offset checkpoints on the same topic; only count committed
        # data in the two Parquet outputs after both availableNow queries finish.
        good = (accepted.writeStream.format("parquet").outputMode("append")
                .option("path", str(output / "accepted"))
                .option("checkpointLocation", str(output / "checkpoints" / "accepted"))
                .trigger(availableNow=True).start())
        bad = (rejected.writeStream.format("parquet").outputMode("append")
               .option("path", str(output / "rejected"))
               .option("checkpointLocation", str(output / "checkpoints" / "rejected"))
               .trigger(availableNow=True).start())
        good.awaitTermination()
        bad.awaitTermination()
        if good.exception() is not None or bad.exception() is not None:
            raise RuntimeError(f"DLQ streaming query failed: {good.exception()} / {bad.exception()}")
        a = spark.read.parquet(str(output / "accepted")).count()
        b = spark.read.parquet(str(output / "rejected")).count()
        print(json.dumps({"accepted": a, "rejected": b,
                          "total_routed": a + b, "output": str(output)}, indent=2))
    finally:
        spark.stop()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--brokers", default="127.0.0.1:9092")
    p.add_argument("--output-dir", default="outputs/spark_dlq_phase5")
    a = p.parse_args()
    run(a.brokers, a.output_dir)


if __name__ == "__main__":
    main()
