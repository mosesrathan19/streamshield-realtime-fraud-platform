"""Phase 2 Kafka producer, topic initialization, and replay-safe consumer.

Optional runtime dependency: pip install '.[kafka]'
Local single-node Kafka only. Kafka offsets are committed AFTER SQLite commits.
A crash between those steps may redeliver messages; event_id uniqueness prevents
duplicate persisted effects. This does NOT claim end-to-end exactly-once delivery.
"""
from __future__ import annotations
import argparse
import json
import time
from .events import events
from .processor import connect
from .rules import evaluate

TRANSACTIONS = "streamshield.transactions.v1"
ALERTS = "streamshield.alerts.v1"
DLQ = "streamshield.invalid.v1"


def _client():
    try:
        from confluent_kafka import Consumer, Producer, KafkaException
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError as exc:
        raise SystemExit("Install Kafka extras first: python -m pip install '.[kafka]'") from exc
    return Consumer, Producer, KafkaException, AdminClient, NewTopic


def init_topics(brokers: str) -> None:
    _, _, _, AdminClient, NewTopic = _client()
    admin = AdminClient({"bootstrap.servers": brokers})
    specs = [NewTopic(TRANSACTIONS, num_partitions=6, replication_factor=1),
             NewTopic(ALERTS, num_partitions=3, replication_factor=1),
             NewTopic(DLQ, num_partitions=3, replication_factor=1)]
    futures = admin.create_topics(specs, request_timeout=20)
    for name, future in futures.items():
        try:
            future.result(timeout=25)
            print(f"Created {name}")
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc).lower():
                raise
            print(f"Exists: {name}")


def produce(brokers: str, count: int, start: int, seed: int) -> dict:
    if count < 0 or start < 0:
        raise ValueError("count and start must be nonnegative")
    _, Producer, _, _, _ = _client()
    producer = Producer({"bootstrap.servers": brokers, "enable.idempotence": True,
                         "acks": "all", "compression.type": "snappy",
                         "linger.ms": 5, "message.timeout.ms": 120000})
    metrics = {"attempted": 0, "acknowledged": 0, "failed": 0}
    def delivery(error, msg):
        if error is not None:
            metrics["failed"] += 1
        else:
            metrics["acknowledged"] += 1
    started = time.perf_counter()
    for item in events(count, seed, start):
        payload = json.dumps(item, separators=(",", ":")).encode()
        while True:
            try:
                producer.produce(TRANSACTIONS, key=item["account_id"], value=payload, on_delivery=delivery)
                break
            except BufferError:
                producer.poll(0.2)
        metrics["attempted"] += 1
        if metrics["attempted"] % 10000 == 0:
            producer.poll(0)
    remaining = producer.flush(120)
    metrics["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    metrics["unflushed"] = remaining
    print(json.dumps(metrics, indent=2))
    if metrics["failed"] or remaining or metrics["acknowledged"] != count:
        raise RuntimeError("Kafka delivery incomplete")
    return metrics


def consume(brokers: str, database: str, group: str, max_messages: int, idle_seconds: int) -> dict:
    if max_messages < 1 or idle_seconds < 1:
        raise ValueError("max_messages and idle_seconds must be positive")
    Consumer, _, _, _, _ = _client()
    consumer = Consumer({"bootstrap.servers": brokers, "group.id": group,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False,
                         "enable.auto.offset.store": False, "max.poll.interval.ms": 300000})
    db = connect(database)
    stats = {"read": 0, "inserted": 0, "duplicates": 0, "invalid": 0, "alerts": 0}
    consumer.subscribe([TRANSACTIONS])
    try:
        last_message_at = time.monotonic()
        while stats["read"] < max_messages and time.monotonic() - last_message_at < idle_seconds:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            last_message_at = time.monotonic()
            stats["read"] += 1
            # Commit SQLite first, then acknowledge consumed Kafka offset.
            # At-least-once delivery with deduplicated durable side effects.
            with db:
                try:
                    event = json.loads(msg.value())
                    reasons = evaluate(event)  # includes version and schema checks
                    cur = db.execute(
                        "INSERT OR IGNORE INTO processed_events (event_id,account_id,event_time,amount_cents) VALUES (?,?,?,?)",
                        (event["event_id"], event["account_id"], event["event_time"], event["amount_cents"]))
                    if cur.rowcount:
                        stats["inserted"] += 1
                        if reasons:
                            db.execute("INSERT INTO risk_alerts VALUES (?,?,?)",
                                       (event["event_id"], event["account_id"], json.dumps(reasons)))
                            stats["alerts"] += 1
                    else:
                        stats["duplicates"] += 1
                except (ValueError, TypeError, KeyError) as exc:
                    # Capture the offset and sanitized error only, never payload.
                    db.execute("INSERT INTO rejected_events (source_line,error) VALUES (?,?)",
                               (msg.offset(), type(exc).__name__))
                    stats["invalid"] += 1
            # Deliberate synchronous commit for clarity; optimize batching in Phase 6.
            consumer.commit(message=msg, asynchronous=False)
        print(json.dumps(stats, indent=2))
        return stats
    finally:
        consumer.close()
        db.close()


def main():
    p = argparse.ArgumentParser(description="StreamShield local Kafka demo")
    p.add_argument("--brokers", default="localhost:9092")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-topics")
    send = sub.add_parser("produce")
    send.add_argument("--count", type=int, default=10000)
    send.add_argument("--start", type=int, default=0)
    send.add_argument("--seed", type=int, default=42)
    recv = sub.add_parser("consume")
    recv.add_argument("--database", default="outputs/kafka_fraud.db")
    recv.add_argument("--group", default="streamshield-local-v1")
    recv.add_argument("--max-messages", type=int, default=10000)
    recv.add_argument("--idle-seconds", type=int, default=20)
    a = p.parse_args()
    if a.cmd == "init-topics":
        init_topics(a.brokers)
    elif a.cmd == "produce":
        produce(a.brokers, a.count, a.start, a.seed)
    else:
        consume(a.brokers, a.database, a.group, a.max_messages, a.idle_seconds)

if __name__ == "__main__":
    main()
