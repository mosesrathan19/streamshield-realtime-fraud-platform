"""Phase 11 isolated 3K synthetic events; never reuse a nonempty Kafka topic."""
from __future__ import annotations

import argparse
import json
from time import monotonic, sleep

from .events import event_at

TOPIC = "streamshield.microbatch_3k.v1"
COUNT = 3000
START = 5_000_000
PARTITIONS = 6


def check_empty_topic(admin, topic: str, partitions: int, timeout: float = 10.0) -> None:
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import OffsetSpec

    meta = admin.list_topics(timeout=timeout).topics[topic]
    if meta.error or len(meta.partitions) != partitions:
        raise RuntimeError("Fixture topic has unhealthy metadata or unexpected partition count")
    requested = {TopicPartition(topic, p): OffsetSpec.earliest() for p in range(partitions)}
    # Read offsets without creating a consumer group or changing broker state.
    low = admin.list_offsets(requested, request_timeout=timeout)
    high = admin.list_offsets(
        {p: OffsetSpec.latest() for p in requested}, request_timeout=timeout
    )
    for partition in requested:
        first = int(low[partition].result(timeout=timeout).offset)
        end = int(high[partition].result(timeout=timeout).offset)
        if first != 0 or end != 0:
            raise RuntimeError(
                f"Fixture topic {topic} partition {partition.partition} has offsets "
                f"{first}..{end}; refusing to publish over existing data"
            )


def publish(brokers: str = "127.0.0.1:9092") -> dict:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    config = {"bootstrap.servers": brokers}
    admin = AdminClient(config)
    existing = admin.list_topics(timeout=15).topics
    if TOPIC not in existing:
        admin.create_topics([NewTopic(TOPIC, PARTITIONS, 1)])[TOPIC].result(timeout=30)
        # Metadata propagation is not instantaneous on some local Kafka setups.
        for attempt in range(8):
            try:
                check_empty_topic(admin, TOPIC, PARTITIONS)
                break
            except (KeyError, RuntimeError):
                if attempt == 7:
                    raise
                sleep(1)
    else:
        check_empty_topic(admin, TOPIC, PARTITIONS)

    sent = {"topic": TOPIC, "attempted": COUNT, "acknowledged": 0, "failed": 0}
    producer = Producer({**config, "acks": "all", "enable.idempotence": True})
    def delivered(err, message):
        sent["failed" if err else "acknowledged"] += 1

    start = monotonic()
    try:
        for index in range(COUNT):
            row = event_at(START + index)
            payload = json.dumps(row, separators=(",", ":")).encode("utf-8")
            while True:
                try:
                    producer.produce(TOPIC, key=row["account_id"].encode("utf-8"),
                                     value=payload, on_delivery=delivered)
                    break
                except BufferError:
                    producer.poll(0.1)
            producer.poll(0)
    finally:
        sent["unflushed"] = producer.flush(60)
    sent["elapsed_seconds"] = round(monotonic() - start, 3)
    print(json.dumps(sent, indent=2), flush=True)
    if sent["failed"] or sent["unflushed"] or sent["acknowledged"] != COUNT:
        raise RuntimeError("Fixture publishing incomplete. Do not blindly rerun.")
    return sent


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="127.0.0.1:9092")
    args = parser.parse_args()
    publish(args.brokers)


if __name__ == "__main__":
    main()
