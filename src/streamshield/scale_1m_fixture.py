"""Phase 6D: isolated, one-time 1M-event Kafka producer with measured acknowledgement rate.

Does not write JSONL files or touch previous benchmark topics or Spark outputs.
Only synthetic event data. A nonempty topic is rejected rather than republished.
"""
from __future__ import annotations
import argparse
import json
import shutil
from pathlib import Path
from time import perf_counter, sleep
from .events import event_at

TOPIC = "streamshield.benchmark_1m.v1"
COUNT = 1_000_000
START = 2_000_000
PARTITIONS = 6
OUTPUT = Path("outputs/spark_benchmark_1m_v1")
MIN_FREE_GIB = 5


def preflight_space() -> dict:
    """Check host filesystem space; Docker VM storage must be checked separately."""
    free = shutil.disk_usage(Path.cwd()).free
    return {"host_free_gib": round(free / (1024 ** 3), 2),
            "min_required_gib": MIN_FREE_GIB, "sufficient": free >= MIN_FREE_GIB * 1024 ** 3}


def publish(brokers: str = "127.0.0.1:9092") -> dict:
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    space = preflight_space()
    if not space["sufficient"]:
        raise RuntimeError(f"Only {space['host_free_gib']} GiB host space free; "
                           f"reserve at least {MIN_FREE_GIB} GiB before 1M test")
    config = {"bootstrap.servers": brokers, "broker.address.family": "v4"}
    admin = AdminClient(config)
    topics = admin.list_topics(timeout=15).topics
    if TOPIC not in topics:
        admin.create_topics([NewTopic(TOPIC, PARTITIONS, 1)])[TOPIC].result(timeout=30)
    else:
        topic = topics[TOPIC]
        if topic.error is not None or len(topic.partitions) != PARTITIONS:
            raise RuntimeError("1M test topic unhealthy or partition count mismatch")
    consumer = Consumer({**config, "group.id": "streamshield-1m-preflight",
                         "enable.auto.commit": False})
    try:
        for p in range(PARTITIONS):
            for retry in range(8):
                try:
                    lo, hi = consumer.get_watermark_offsets(
                        TopicPartition(TOPIC, p), timeout=5, cached=False)
                    break
                except Exception as exc:
                    if retry == 7:
                        raise RuntimeError(f"Cannot verify 1M topic partition {p}") from exc
                    sleep(2)
            if lo != 0 or hi != 0:
                raise RuntimeError(
                    f"1M topic partition {p} already contains offsets {lo}..{hi}; "
                    "do not republish or reset the topic")
    finally:
        consumer.close()

    producer = Producer({**config, "acks": "all", "enable.idempotence": True,
                         "compression.type": "lz4", "linger.ms": 10,
                         "queue.buffering.max.messages": 100000})
    result = {"topic": TOPIC, "attempted": COUNT, "acknowledged": 0,
              "failed": 0, "partitions": PARTITIONS}
    def delivery(error, message):
        result["failed" if error is not None else "acknowledged"] += 1
    start = perf_counter()
    try:
        for i in range(START, START + COUNT):
            event = event_at(i)
            raw = json.dumps(event, separators=(",", ":")).encode("utf-8")
            while True:
                try:
                    producer.produce(TOPIC, value=raw,
                                     key=event["account_id"].encode("utf-8"),
                                     on_delivery=delivery)
                    break
                except BufferError:
                    producer.poll(0.1)
            # Frequent callbacks keep the producer queue bounded.
            producer.poll(0)
    finally:
        result["unflushed"] = producer.flush(180)
    elapsed = perf_counter() - start
    result["producer_elapsed_seconds"] = round(elapsed, 3)
    result["acknowledged_events_per_second"] = round(result["acknowledged"] / elapsed, 1)
    result["host_free_gib_before"] = space["host_free_gib"]
    print(json.dumps(result, indent=2), flush=True)
    if result["failed"] or result["unflushed"] or result["acknowledged"] != COUNT:
        raise RuntimeError("1M publication not fully acknowledged; DO NOT republish blindly")
    # Save separate publisher metrics to an exclusive output directory, but
    # do not create Spark accepted/checkpoint locations.
    OUTPUT.mkdir(parents=True, exist_ok=True)
    report = OUTPUT / "producer_result.json"
    with report.open("x", encoding="utf-8") as file:
        json.dump(result, file, indent=2)
        file.write("\n")
    print(f"Saved Kafka producer report: {report.resolve()}", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="127.0.0.1:9092")
    parser.add_argument("--check-space", action="store_true",
                        help="check host filesystem space, do not publish")
    args = parser.parse_args()
    if args.check_space:
        print(json.dumps(preflight_space(), indent=2))
    else:
        publish(args.brokers)


if __name__ == "__main__":
    main()
