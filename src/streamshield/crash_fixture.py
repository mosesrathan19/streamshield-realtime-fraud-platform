"""Isolated, one-time Kafka fixture for local Spark forced-crash recovery."""
from __future__ import annotations
import argparse
import json
import time
from .events import event_at

TOPIC = "streamshield.crash_test.v1"
COUNT = 300
START = 40000


def payloads():
    return [json.dumps(event_at(i), separators=(",", ":")).encode("utf-8")
            for i in range(START, START + COUNT)]


def send(brokers="127.0.0.1:9092"):
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    config = {"bootstrap.servers": brokers, "broker.address.family": "v4"}
    admin = AdminClient(config)
    topics = admin.list_topics(timeout=10).topics
    if TOPIC not in topics:
        admin.create_topics([NewTopic(TOPIC, 1, 1)])[TOPIC].result(timeout=20)
    elif topics[TOPIC].error is not None or len(topics[TOPIC].partitions) != 1:
        raise RuntimeError("Crash-test topic must have one healthy partition")
    consumer = Consumer({**config, "group.id": "crash-fixture-preflight",
                         "enable.auto.commit": False})
    try:
        for attempt in range(6):
            try:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(TOPIC, 0), timeout=5, cached=False)
                break
            except Exception as exc:
                if attempt == 5:
                    raise RuntimeError("Could not inspect fixture offsets; no events sent") from exc
                time.sleep(2)
    finally:
        consumer.close()
    if (low, high) != (0, 0):
        raise RuntimeError(f"Fixture topic already has offset range {low}..{high}; "
                           "refusing to republish or reset")
    producer = Producer({**config, "acks": "all", "enable.idempotence": True})
    metrics = {"attempted": COUNT, "acknowledged": 0, "failed": 0}
    def delivery(err, msg):
        metrics["failed" if err is not None else "acknowledged"] += 1
    for i, payload in enumerate(payloads()):
        producer.produce(TOPIC, key=f"crash-{i}", value=payload, on_delivery=delivery)
        producer.poll(0)
    metrics["unflushed"] = producer.flush(60)
    metrics["topic"] = TOPIC
    print(json.dumps(metrics, indent=2))
    if metrics["failed"] or metrics["unflushed"] or metrics["acknowledged"] != COUNT:
        raise RuntimeError("Crash-test fixture was not fully delivered")
    return metrics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="127.0.0.1:9092")
    args = parser.parse_args()
    send(args.brokers)


if __name__ == "__main__":
    main()
