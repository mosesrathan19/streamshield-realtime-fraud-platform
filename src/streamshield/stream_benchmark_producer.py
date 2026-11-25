"""Phase 6 isolated Kafka producer benchmark: a 10K canary then 90K extension.

Only publishes to streamshield.benchmark_100k.v1; refuses to publish if
the topic's retained offsets aren't exactly those expected for that stage.
Producer duration includes Python event generation, serialization and Kafka
delivery acknowledgement, but no Spark processing.
"""
from __future__ import annotations

import argparse
import json
import time
from .events import events

TOPIC = "streamshield.benchmark_100k.v1"
SEED = 42
START = 1000000
STAGES = {"canary": (START, 10000, 0),
          "extend": (START + 10000, 90000, 10000)}


def stage_config(stage: str) -> tuple[int, int, int]:
    if stage not in STAGES:
        raise ValueError("stage must be canary or extend")
    return STAGES[stage]


def send(stage: str, brokers: str = "127.0.0.1:9092") -> dict:
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic

    start, count, expected_offset_total = stage_config(stage)
    config = {"bootstrap.servers": brokers, "broker.address.family": "v4"}
    admin = AdminClient(config)
    topics = admin.list_topics(timeout=15).topics
    if TOPIC not in topics:
        if stage != "canary":
            raise RuntimeError("benchmark topic not initialized: run canary first")
        admin.create_topics([NewTopic(TOPIC, num_partitions=6, replication_factor=1)])[TOPIC].result(timeout=20)
    elif topics[TOPIC].error is not None or len(topics[TOPIC].partitions) != 6:
        raise RuntimeError("benchmark topic must have exactly six healthy partitions")

    probe = Consumer({**config, "group.id": "bench100k-offset-probe",
                      "enable.auto.commit": False})
    try:
        for attempt in range(6):
            try:
                ranges = [probe.get_watermark_offsets(
                    TopicPartition(TOPIC, partition), timeout=5, cached=False)
                    for partition in range(6)]
                break
            except Exception as exc:
                if attempt == 5:
                    raise RuntimeError("Benchmark offset preflight unavailable: nothing sent") from exc
                time.sleep(2)
    finally:
        probe.close()
    first_offsets = sum(low for low, high in ranges)
    last_offsets = sum(high for low, high in ranges)
    if first_offsets != 0 or last_offsets != expected_offset_total:
        raise RuntimeError(
            f"Expected retained topic offsets 0..{expected_offset_total}, "
            f"got lows sum={first_offsets}, highs sum={last_offsets}; "
            "do not republish or delete the test topic"
        )

    producer = Producer({**config, "enable.idempotence": True, "acks": "all",
                         "compression.type": "snappy", "linger.ms": 5,
                         "message.timeout.ms": 120000})
    result = {"stage": stage, "topic": TOPIC, "attempted": 0,
              "acknowledged": 0, "failed": 0}
    def on_delivery(error, _message):
        result["failed" if error is not None else "acknowledged"] += 1

    started = time.perf_counter()
    for item in events(count=count, seed=SEED, start=start):
        payload = json.dumps(item, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                producer.produce(TOPIC, key=item["account_id"],
                                 value=payload, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.2)
        result["attempted"] += 1
        if result["attempted"] % 5000 == 0:
            producer.poll(0)
    remaining = producer.flush(120)
    seconds = time.perf_counter() - started
    result.update({"unflushed": remaining, "elapsed_seconds": round(seconds, 3),
                   "acknowledged_per_second": round(result["acknowledged"] / seconds, 1)})
    print(json.dumps(result, indent=2))
    if remaining or result["failed"] or result["acknowledged"] != count:
        raise RuntimeError("Producer acknowledgements incomplete; do not rerun blindly")
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("stage", choices=("canary", "extend"))
    p.add_argument("--brokers", default="127.0.0.1:9092")
    a = p.parse_args()
    send(a.stage, a.brokers)


if __name__ == "__main__":
    main()
