"""Read-only Kafka consumer-group offset inspection.

No consumer is instantiated, no group membership is created, no offsets are
committed, and no topic data is consumed. Spark Structured Streaming offsets
are checkpointed separately and may not appear as Kafka group commits.
"""
from __future__ import annotations

import os


def calculate_lag(earliest: int, latest: int, committed: int | None) -> tuple[int | None, str]:
    """Only compute comparable Kafka group lag for an in-range committed offset."""
    if committed is None or committed < 0:
        return None, "No committed offset"
    if earliest < 0 or latest < earliest:
        return None, "Invalid broker offset range"
    if committed < earliest:
        return None, "Commit before retained range"
    if committed > latest:
        return None, "Commit beyond log end"
    return latest - committed, "Comparable snapshot"


def kafka_consumer_groups(
    bootstrap: str | None = None,
    *,
    prefix: str = "streamshield.",
    timeout: float = 5.0,
    max_groups: int = 30,
    admin=None,
) -> dict:
    """Discover existing groups and calculate per-partition lag where possible.

    Returns group_count, rows, skipped_groups. Only committed offsets for
    matching streamshield.* topics are displayed; a group with no commits for
    these topics is listed in group_count but not rows.
    """
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec

    address = bootstrap or os.getenv("STREAMSHIELD_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
    client = admin if admin is not None else AdminClient({
        "bootstrap.servers": address,
        "socket.timeout.ms": int(timeout * 1000),
        "client.id": "streamshield-readonly-group-inspector",
    })
    result = client.list_consumer_groups(request_timeout=timeout).result(timeout=timeout)
    if result.errors:
        raise RuntimeError("Kafka consumer-group listing returned broker errors")
    groups = sorted(result.valid, key=lambda x: x.group_id)
    if len(groups) > max_groups:
        # Bounded inspection; never claim all groups were inspected.
        chosen = groups[:max_groups]
    else:
        chosen = groups

    rows = []
    for group in chosen:
        group_id = group.group_id
        # Confluent's current API accepts ONE group per list offsets call.
        result_map = client.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group_id)], request_timeout=timeout
        )
        committed = result_map[group_id].result(timeout=timeout)
        partitions = [
            part for part in committed.topic_partitions
            if part.topic.startswith(prefix)
        ]
        if not partitions:
            continue

        valid = [p for p in partitions if p.error is None and p.offset is not None and p.offset >= 0]
        earliest = {}
        latest = {}
        if valid:
            earliest = client.list_offsets(
                {TopicPartition(p.topic, p.partition): OffsetSpec.earliest() for p in valid},
                request_timeout=timeout,
            )
            latest = client.list_offsets(
                {TopicPartition(p.topic, p.partition): OffsetSpec.latest() for p in valid},
                request_timeout=timeout,
            )

        for p in sorted(partitions, key=lambda x: (x.topic, x.partition)):
            committed_offset = int(p.offset) if p.offset is not None and p.offset >= 0 and p.error is None else None
            first = end = None
            if committed_offset is not None:
                key = TopicPartition(p.topic, p.partition)
                first = int(earliest[key].result(timeout=timeout).offset)
                end = int(latest[key].result(timeout=timeout).offset)
            if p.error is not None:
                lag, status = None, "Partition offset error"
            elif committed_offset is None:
                lag, status = None, "No committed offset"
            else:
                lag, status = calculate_lag(first, end, committed_offset)
            rows.append({
                "group": group_id,
                "group_state": str(getattr(group, "state", None) or "Unknown"),
                "topic": p.topic,
                "partition": p.partition,
                "earliest": first,
                "latest": end,
                "committed": committed_offset,
                "lag": lag,
                "status": status,
            })
    return {
        "group_count": len(groups),
        "inspected_groups": len(chosen),
        "skipped_groups": max(0, len(groups) - len(chosen)),
        "rows": rows,
    }
