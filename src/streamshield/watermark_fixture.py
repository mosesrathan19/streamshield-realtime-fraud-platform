"""Isolated two-stage Kafka fixture for testing Spark event-time watermark behavior.

A new dedicated one-partition topic and isolated Spark checkpoint are required.
Does not publish to the primary StreamShield transactions topic.
"""
from __future__ import annotations

import argparse
import json
import time
from .events import event_at

TOPIC = "streamshield.watermark_test.v1"

def fixture(stage: str) -> list[dict]:
    def make(i: int, event_id: str, ts: str):
        row = event_at(i)
        row["event_id"] = event_id
        row["transaction_id"] = "txn_" + event_id
        row["event_time"] = ts
        return row
    seed1 = make(20001, "wm_seed_1", "2026-01-01T00:30:00+00:00")
    if stage == "seed":
        return [seed1, make(20002, "wm_seed_2", "2026-01-01T00:31:00+00:00")]
    if stage == "followup":
        return [
            make(20003, "wm_too_late", "2026-01-01T00:00:00+00:00"),
            make(20004, "wm_outorder", "2026-01-01T00:25:00+00:00"),
            make(20005, "wm_on_time", "2026-01-01T00:32:00+00:00"),
            seed1,
        ]
    raise ValueError("unknown stage")


def send(stage: str, brokers: str, topic: str = TOPIC) -> dict:
    from confluent_kafka import Producer, Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic

    if topic != TOPIC:
        raise ValueError("This fixture is restricted to its own dedicated topic")
    admin = AdminClient({"bootstrap.servers": brokers, "broker.address.family": "v4"})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        futures[topic].result(timeout=15)
    else:
        meta = existing[topic]
        if meta.error is not None or len(meta.partitions) != 1:
            raise RuntimeError("fixture topic must exist as a healthy single partition")
    check = Consumer({"bootstrap.servers": brokers, "broker.address.family": "v4",
                      "group.id": "wm-fixture-preflight",
                      "enable.auto.commit": False})
    try:
        # The topic leader may still be initializing immediately after create_topics.
        # Retry transport/leader errors, but NEVER skip the offset guard.
        last_error = None
        for attempt in range(6):
            try:
                low, high = check.get_watermark_offsets(
                    TopicPartition(topic, 0), timeout=5, cached=False
                )
                break
            except Exception as exc:
                last_error = exc
                if attempt == 5:
                    raise RuntimeError(
                        "Could not read Kafka fixture offsets after six attempts; "
                        "nothing was published. Check Docker broker health."
                    ) from last_error
                time.sleep(2)
    finally:
        check.close()
    target = 0 if stage == "seed" else 2
    if low != 0 or high != target:
        raise RuntimeError(f"expected topic offset range 0..{target} before {stage}; "
                           f"found {low}..{high}. Do not republish; inspect test state.")
    messages = fixture(stage)
    metrics = {"attempted": len(messages), "acknowledged": 0, "failed": 0}
    producer = Producer({"bootstrap.servers": brokers, "broker.address.family": "v4",
                         "enable.idempotence": True,
                         "acks": "all"})
    def callback(error, message):
        metrics["failed" if error is not None else "acknowledged"] += 1
    for row in messages:
        payload = json.dumps(row).encode("utf-8")
        while True:
            try:
                producer.produce(topic, key=row["account_id"], value=payload,
                                 on_delivery=callback)
                break
            except BufferError:
                producer.poll(0.2)
    metrics["unflushed"] = producer.flush(30)
    metrics.update({"stage": stage, "topic": topic})
    print(json.dumps(metrics, indent=2))
    if metrics["failed"] or metrics["unflushed"] or metrics["acknowledged"] != len(messages):
        raise RuntimeError("fixture delivery did not complete")
    return metrics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=("seed", "followup"))
    parser.add_argument("--brokers", default="127.0.0.1:9092")
    args = parser.parse_args()
    send(args.stage, args.brokers)


if __name__ == "__main__":
    main()
