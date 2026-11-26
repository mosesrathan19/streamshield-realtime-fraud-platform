"""Phase 3: local Kafka -> PySpark Structured Streaming demo.

Requires Spark 3.5.6 and the matching Kafka connector (see docs/PHASE3_SPARK.md).
Two independent streaming queries:
1. Parse/validate transactions, bounded event_id deduplication -> Parquet.
2. Event-time window counts -> console (update mode).
Both have independent checkpoints; window metrics are not guaranteed to be
identical to deduplicated sink counts because the second query has independent
watermark/dedup state and update-mode intermediate revisions.

Intended for local experimentation only, not production fraud processing.
"""
from __future__ import annotations
import argparse
from pathlib import Path

SPARK_VERSION = "3.5.6"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6"
TOPIC = "streamshield.transactions.v1"


def schema():
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   LongType, IntegerType, BooleanType)
    return StructType([
        StructField("event_id", StringType()),
        StructField("transaction_id", StringType()),
        StructField("account_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_category", StringType()),
        StructField("amount_cents", LongType()),
        StructField("currency", StringType()),
        StructField("country", StringType()),
        StructField("event_time", StringType()),
        StructField("event_type", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("synthetic_label", BooleanType()),
    ])


def parsed_stream(spark, brokers: str, topic: str = TOPIC,
                  max_offsets_per_trigger: int | None = None):
    from pyspark.sql import functions as F
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", brokers)
              .option("subscribe", topic)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "true"))
    if max_offsets_per_trigger is not None:
        if max_offsets_per_trigger < 1:
            raise ValueError("max_offsets_per_trigger must be positive")
        reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    kafka = reader.load()
    # Keep Kafka metadata to assist troubleshooting and reconciliation.
    parsed = kafka.select(
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.from_json(F.col("value").cast("string"), schema()).alias("payload"),
    ).select("kafka_partition", "kafka_offset", "payload.*")
    # Explicitly ignore malformed/incomplete rows in the accepted data flow.
    # Phase 5 will add a separate dead-letter stream with durable raw message refs.
    valid = parsed.filter(
        F.col("event_id").isNotNull()
        & F.col("account_id").isNotNull()
        & F.col("transaction_id").isNotNull()
        & F.col("merchant_id").isNotNull()
        & (F.col("amount_cents") > 0)
        & (F.col("currency") == "USD")
        & (F.col("event_type") == "payment_authorized")
        & (F.col("schema_version") == 1)
        & F.col("event_time").isNotNull()
    )
    return (valid.withColumn("event_ts", F.to_timestamp("event_time"))
            .filter(F.col("event_ts").isNotNull()))


def enriched_stream(spark, brokers: str, watermark: str = "10 minutes",
                    topic: str = TOPIC, max_offsets_per_trigger: int | None = None):
    from pyspark.sql import functions as F
    accepted = (parsed_stream(spark, brokers, topic, max_offsets_per_trigger)
                .withWatermark("event_ts", watermark)
                .dropDuplicatesWithinWatermark(["event_id"]))
    return accepted.withColumn(
        "risk_high_amount", F.col("amount_cents") >= 250000
    ).withColumn(
        "risk_synthetic_country", F.col("country") == "ZZ"
    ).withColumn(
        "risk_flag", F.col("risk_high_amount") | F.col("risk_synthetic_country")
    )


def run(brokers: str, output_dir: str, spark_master: str, watermark: str,
        topic: str = TOPIC, skip_windows: bool = False):
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.master(spark_master)
             .appName("StreamShieldStructuredStreaming")
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    if spark.version != SPARK_VERSION:
        raise RuntimeError(f"Expected Spark {SPARK_VERSION}; got {spark.version}")
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    try:
        accepted = enriched_stream(spark, brokers, watermark, topic)
        # Built-in Parquet sink with durable checkpoint. Use a fresh checkpoint
        # when changing source/query logic. Do not erase checkpoints during replay.
        saved = (accepted.writeStream.format("parquet")
                 .outputMode("append")
                 .option("path", str((output / "accepted").resolve()))
                 .option("checkpointLocation", str((output / "checkpoints" / "accepted").resolve()))
                 .trigger(availableNow=True).start())
        # For isolated watermark tests, only the persisted Parquet query runs.
        # This avoids creating unrelated console-query checkpoints.
        visible = None
        if not skip_windows:
            windows = (enriched_stream(spark, brokers, watermark, topic)
                       .groupBy(F.window("event_ts", "1 minute"), F.col("risk_flag"))
                       .agg(F.count("*").alias("transactions")))
            visible = (windows.writeStream.format("console")
                       .outputMode("update")
                       .option("truncate", "false")
                       .option("numRows", 100)
                       .option("checkpointLocation", str((output / "checkpoints" / "windows").resolve()))
                       .trigger(availableNow=True).start())
        saved.awaitTermination()
        if visible is not None:
            visible.awaitTermination()
        if saved.exception() is not None:
            raise RuntimeError(str(saved.exception()))
        if visible is not None and visible.exception() is not None:
            raise RuntimeError(str(visible.exception()))
        # Read the persisted accepted stream and print verifiable final metrics.
        df = spark.read.parquet(str((output / "accepted").resolve()))
        result = df.agg(
            F.count("*").alias("persisted_events"),
            F.sum(F.col("risk_flag").cast("long")).alias("synthetic_alerts"),
            F.countDistinct("event_id").alias("distinct_event_ids"),
        ).first()
        print({"persisted_events": result["persisted_events"],
               "distinct_event_ids": result["distinct_event_ids"],
               "synthetic_alerts": result["synthetic_alerts"],
               "source_topic": topic, "watermark": watermark,
               "output": str(output.resolve())})
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="127.0.0.1:9092")
    parser.add_argument("--output-dir", default="outputs/spark_phase3")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--topic", default=TOPIC,
                        help="Kafka source topic; use a fresh checkpoint when changing topic")
    parser.add_argument("--skip-windows", action="store_true",
                        help="Run only Parquet sink; useful for isolated watermark tests")
    args = parser.parse_args()
    run(args.brokers, args.output_dir, args.master, args.watermark,
        args.topic, args.skip_windows)


if __name__ == "__main__":
    main()
