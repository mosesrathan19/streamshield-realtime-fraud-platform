"""Offline tests of read-only Kafka offset inspection. No broker required."""
from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))

from kafka_observability import kafka_topic_offsets


class FakeTopicPartition:
    def __init__(self, topic, partition):
        self.topic, self.partition = topic, partition

    def __eq__(self, other):
        return (isinstance(other, FakeTopicPartition)
                and (self.topic, self.partition) == (other.topic, other.partition))

    def __hash__(self):
        return hash((self.topic, self.partition))


class FakeOffsetSpec:
    @staticmethod
    def earliest():
        return "earliest"

    @staticmethod
    def latest():
        return "latest"


class FakeFuture:
    def __init__(self, offset):
        self.offset = offset

    def result(self, timeout=None):
        return types.SimpleNamespace(offset=self.offset)


class FakeAdmin:
    def __init__(self, invalid=False):
        self.calls = []
        self.invalid = invalid

    def list_topics(self, timeout=None):
        self.calls.append("metadata")
        return types.SimpleNamespace(topics={
            "streamshield.transactions": types.SimpleNamespace(
                error=None, partitions={
                    0: types.SimpleNamespace(error=None),
                    1: types.SimpleNamespace(error=None),
                }),
            "unrelated": types.SimpleNamespace(
                error=None, partitions={0: types.SimpleNamespace(error=None)}),
        })

    def list_offsets(self, offsets, request_timeout=None):
        kind = set(offsets.values()).pop()
        self.calls.append(kind)
        self.assert_only_streamshield(offsets)
        return {
            p: FakeFuture(
                ((100 if p.partition == 0 else 50) if kind == "earliest"
                 else (150 if p.partition == 0 else (20 if self.invalid else 90))))
            for p in offsets
        }

    @staticmethod
    def assert_only_streamshield(offsets):
        assert all(p.topic == "streamshield.transactions" for p in offsets)


class KafkaOffsetTests(unittest.TestCase):
    def setUp(self):
        mock_confluent = types.ModuleType("confluent_kafka")
        mock_confluent.TopicPartition = FakeTopicPartition
        mock_admin = types.ModuleType("confluent_kafka.admin")
        mock_admin.AdminClient = lambda config: FakeAdmin()
        mock_admin.OffsetSpec = FakeOffsetSpec
        self.modules_patch = patch.dict(
            sys.modules,
            {"confluent_kafka": mock_confluent, "confluent_kafka.admin": mock_admin},
        )
        self.modules_patch.start()
        self.addCleanup(self.modules_patch.stop)

    def test_offset_spans_and_read_only_api_calls(self):
        admin = FakeAdmin()
        rows = kafka_topic_offsets(admin=admin)
        self.assertEqual(admin.calls, ["metadata", "earliest", "latest"])
        self.assertEqual([(r["partition"], r["earliest"], r["latest"], r["offset_span"])
                          for r in rows], [(0, 100, 150, 50), (1, 50, 90, 40)])

    def test_inconsistent_broker_offsets_are_not_silently_reported(self):
        with self.assertRaises(ValueError):
            kafka_topic_offsets(admin=FakeAdmin(invalid=True))


if __name__ == "__main__":
    unittest.main()
