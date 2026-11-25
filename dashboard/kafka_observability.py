"""Read-only Kafka topic/partition diagnostics.

Queries Kafka metadata and log offsets through AdminClient. Does not subscribe,
join a consumer group, consume events, commit offsets, or create topics.

Latest-earliest is a retained offset SPAN, NOT a message count or consumer lag:
retention, compaction, and gaps can make it differ from actual record count.
"""
from __future__ import annotations

import os


def kafka_topic_offsets(
    bootstrap: str | None = None,
    *,
    prefix: str = "streamshield.",
    timeout: float = 5.0,
    admin=None,
) -> list[dict]:
    """Return read-only per-partition offsets for existing matching Kafka topics.

    Optional admin injection lets unit tests provide a fake without any broker.
    """
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec

    address = bootstrap or os.getenv("STREAMSHIELD_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
    client = admin if admin is not None else AdminClient({
        "bootstrap.servers": address,
        "socket.timeout.ms": int(timeout * 1000),
        "client.id": "streamshield-readonly-observer",
    })

    # list_topics() with no topic name cannot auto-create unknown topics.
    metadata = client.list_topics(timeout=timeout)
    partitions = []
    for name, topic in sorted(metadata.topics.items()):
        if not name.startswith(prefix) or topic.error is not None:
            continue
        for partition_id, part in sorted(topic.partitions.items()):
            if part.error is not None:
                continue
            partitions.append(TopicPartition(name, partition_id))

    if not partitions:
        return []

    earliest = client.list_offsets(
        {p: OffsetSpec.earliest() for p in partitions},
        request_timeout=timeout,
    )
    latest = client.list_offsets(
        {p: OffsetSpec.latest() for p in partitions},
        request_timeout=timeout,
    )

    result = []
    for partition in partitions:
        first = int(earliest[partition].result(timeout=timeout).offset)
        end = int(latest[partition].result(timeout=timeout).offset)
        if first < 0 or end < first:
            raise ValueError(
                f"Invalid broker offsets: {partition.topic}/{partition.partition}: {first}, {end}"
            )
        result.append({
            "topic": partition.topic,
            "partition": partition.partition,
            "earliest": first,
            "latest": end,
            "offset_span": end - first,
        })
    return result
