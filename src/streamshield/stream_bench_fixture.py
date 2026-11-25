"""Phase 6: publish one isolated 100K-event synthetic Kafka streaming fixture.

The producer refuses to publish to a nonempty topic: no hidden benchmark replays.
Elapsed time includes producer construction, delivery callbacks and final flush,
but excludes topic creation and the Kafka preflight offset checks.
"""
from __future__ import annotations

import argparse
import json
from time import perf_counter, sleep
from .events import event_at

TOPIC = "streamshield.benchmark_100k.v1"
COUNT = 100_000
START = 500_000
PARTITIONS = 6
TOPIC_V2 = "streamshield.benchmark_100k.v2"


def expected_id(i: int) -> str:
    return f"evt_{START + i:012d}"


def fixture_event(i: int) -> dict:
    if not 0 <= i < COUNT:
        raise ValueError("fixture event index out of range")
    return event_at(START + i)


def publish(brokers: str = "127.0.0.1:9092", trial: str = "v1") -> dict:
    if trial not in ("v1", "v2"):
        raise ValueError("Trial must be v1 or v2")
    topic_name = TOPIC if trial == "v1" else TOPIC_V2
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic

    config = {"bootstrap.servers": brokers, "broker.address.family": "v4"}
    admin = AdminClient(config)
    existing = admin.list_topics(timeout=15).topics
    if topic_name not in existing:
        admin.create_topics([NewTopic(topic_name, PARTITIONS, 1)])[topic_name].result(timeout=30)
    else:
        topic = existing[topic_name]
        if topic.error is not None or len(topic.partitions) != PARTITIONS:
            raise RuntimeError("Benchmark topic not healthy or partition count mismatch")

    consumer = Consumer({**config, "group.id": "stream-benchmark-preflight",
                         "enable.auto.commit": False})
    try:
        # Newly created topics sometimes need a short metadata propagation delay.
        for partition in range(PARTITIONS):
            for retry in range(8):
                try:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(topic_name, partition), timeout=5, cached=False)
                    break
                except Exception as exc:
                    if retry == 7:
                        raise RuntimeError(
                            f"Unable to inspect benchmark topic partition {partition}") from exc
                    sleep(2)
            if low != 0 or high != 0:
                raise RuntimeError(
                    f"Benchmark topic already contains records at partition {partition}: "
                    f"offset range {low}..{high}; refusing to publish twice")
    finally:
        consumer.close()

    result = {"topic": topic_name, "attempted": COUNT, "acknowledged": 0,
              "failed": 0, "partitions": PARTITIONS}
    producer = Producer({**config, "acks": "all", "enable.idempotence": True,
                         "compression.type": "lz4", "linger.ms": 10,
                         "queue.buffering.max.messages": 200000})
    def on_delivery(error, message):
        result["failed" if error is not None else "acknowledged"] += 1

    begin = perf_counter()
    try:
        for index in range(COUNT):
            payload = fixture_event(index)
            encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            key = payload["account_id"].encode("utf-8")
            while True:
                try:
                    producer.produce(topic_name, key=key, value=encoded,
                                     on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.1)
            producer.poll(0)
    finally:
        result["unflushed"] = producer.flush(120)
    elapsed = perf_counter() - begin
    result["producer_elapsed_seconds"] = round(elapsed, 3)
    result["acknowledged_events_per_second"] = (
        round(result["acknowledged"] / elapsed, 1) if elapsed else None)
    print(json.dumps(result, indent=2))
    if (result["failed"] != 0 or result["unflushed"] != 0
            or result["acknowledged"] != COUNT):
        raise RuntimeError("Kafka 100K benchmark fixture was not fully acknowledged")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="127.0.0.1:9092")
    parser.add_argument("--trial", choices=["v1", "v2"], default="v1")
    args = parser.parse_args()
    publish(args.brokers, args.trial)


if __name__ == "__main__":
    main()
