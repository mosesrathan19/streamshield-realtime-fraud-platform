"""Phase 5D isolated Kafka fixture containing 2 good and 4 bad messages.

Refuses to send if topic already has records. Never touches primary topic.
"""
from __future__ import annotations
import argparse
import json
import time
from .events import event_at
from .dlq_stream import TOPIC


def fixture() -> list[bytes]:
    good_a = event_at(31001)
    good_b = event_at(31002)
    good_a["event_id"] = "dlq_good_1"
    good_b["event_id"] = "dlq_good_2"
    bad_amount = dict(event_at(31003), event_id="dlq_bad_amount", amount_cents=-5)
    bad_account = dict(event_at(31004), event_id="dlq_bad_account")
    bad_account.pop("account_id")
    bad_time = dict(event_at(31005), event_id="dlq_bad_time", event_time="not-a-timestamp")
    entries = [good_a, good_b, bad_amount, bad_account, bad_time]
    return [json.dumps(x).encode("utf-8") for x in entries] + [b"{bad JSON"]


def send(brokers: str):
    from confluent_kafka import Producer, Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    config = {"bootstrap.servers": brokers, "broker.address.family": "v4"}
    admin = AdminClient(config)
    topics = admin.list_topics(timeout=10).topics
    if TOPIC not in topics:
        admin.create_topics([NewTopic(TOPIC, 1, 1)])[TOPIC].result(timeout=20)
    elif topics[TOPIC].error is not None or len(topics[TOPIC].partitions) != 1:
        raise RuntimeError("DLQ test topic must have one healthy partition")
    check = Consumer({**config, "group.id": "dlq-test-preflight", "enable.auto.commit": False})
    try:
        for attempt in range(6):
            try:
                low, high = check.get_watermark_offsets(
                    TopicPartition(TOPIC, 0), timeout=5, cached=False)
                break
            except Exception as exc:
                if attempt == 5:
                    raise RuntimeError("DLQ topic offset check unavailable; nothing published") from exc
                time.sleep(2)
    finally:
        check.close()
    if (low, high) != (0, 0):
        raise RuntimeError(f"DLQ topic already contains offsets {low}..{high}; refusing republish")
    values = fixture()
    producer = Producer({**config, "enable.idempotence": True, "acks": "all"})
    result = {"attempted": len(values), "acknowledged": 0, "failed": 0}
    def callback(error, msg):
        result["failed" if error else "acknowledged"] += 1
    for index, value in enumerate(values):
        producer.produce(TOPIC, key=f"dlq-test-{index}", value=value, on_delivery=callback)
    result["unflushed"] = producer.flush(30)
    result["topic"] = TOPIC
    print(json.dumps(result, indent=2))
    if result["failed"] or result["unflushed"] or result["acknowledged"] != len(values):
        raise RuntimeError("Kafka fixture publishing incomplete")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--brokers", default="127.0.0.1:9092")
    send(p.parse_args().brokers)


if __name__ == "__main__":
    main()
