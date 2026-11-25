"""Offline tests of read-only consumer group offset inspection."""
from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))
from kafka_groups import calculate_lag, kafka_consumer_groups


class FakeTopicPartition:
    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition

    def __hash__(self):
        return hash((self.topic, self.partition))

    def __eq__(self, other):
        return isinstance(other, FakeTopicPartition) and (self.topic, self.partition) == (other.topic, other.partition)


class FakeSpec:
    @staticmethod
    def earliest():
        return "earliest"

    @staticmethod
    def latest():
        return "latest"


class FakeGroup:
    def __init__(self, group_id):
        self.group_id = group_id
        self.topic_partitions = None


class FakeFuture:
    def __init__(self, value):
        self.value = value

    def result(self, timeout=None):
        return self.value


class FakeAdmin:
    def __init__(self, has_commit=True):
        self.calls = []
        self.has_commit = has_commit

    def list_consumer_groups(self, request_timeout=None):
        self.calls.append("list_groups")
        return FakeFuture(types.SimpleNamespace(valid=[types.SimpleNamespace(group_id="demo", state="EMPTY")], errors=[]))

    def list_consumer_group_offsets(self, request, request_timeout=None):
        self.calls.append("read_commits")
        assert len(request) == 1 and request[0].group_id == "demo"
        item = types.SimpleNamespace(topic="streamshield.transactions", partition=0,
                                     offset=12 if self.has_commit else -1, error=None)
        untracked = types.SimpleNamespace(topic="external.topic", partition=0, offset=10, error=None)
        return {"demo": FakeFuture(types.SimpleNamespace(topic_partitions=[item, untracked]))}

    def list_offsets(self, specs, request_timeout=None):
        kind = next(iter(specs.values()))
        self.calls.append(kind)
        offset = 5 if kind == "earliest" else 20
        return {p: FakeFuture(types.SimpleNamespace(offset=offset)) for p in specs}


class KafkaGroupTests(unittest.TestCase):
    def setUp(self):
        mod = types.ModuleType("confluent_kafka")
        mod.TopicPartition = FakeTopicPartition
        mod.ConsumerGroupTopicPartitions = FakeGroup
        adm = types.ModuleType("confluent_kafka.admin")
        adm.AdminClient = lambda args: FakeAdmin()
        adm.OffsetSpec = FakeSpec
        patcher = patch.dict(sys.modules, {"confluent_kafka": mod, "confluent_kafka.admin": adm})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_calculate_lag_bounded(self):
        self.assertEqual(calculate_lag(5, 20, 12), (8, "Comparable snapshot"))
        self.assertEqual(calculate_lag(5, 20, None), (None, "No committed offset"))
        self.assertEqual(calculate_lag(5, 20, 3), (None, "Commit before retained range"))
        self.assertEqual(calculate_lag(5, 20, 25), (None, "Commit beyond log end"))

    def test_read_only_admin_requests(self):
        admin = FakeAdmin()
        result = kafka_consumer_groups(admin=admin)
        self.assertEqual(admin.calls, ["list_groups", "read_commits", "earliest", "latest"])
        self.assertEqual(result["group_count"], 1)
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0]["lag"], 8)
        self.assertEqual(result["rows"][0]["status"], "Comparable snapshot")

    def test_missing_commits_are_not_lag_zero(self):
        admin = FakeAdmin(has_commit=False)
        result = kafka_consumer_groups(admin=admin)
        self.assertEqual(admin.calls, ["list_groups", "read_commits"])
        self.assertIsNone(result["rows"][0]["lag"])
        self.assertEqual(result["rows"][0]["status"], "No committed offset")

    def test_empty_cluster_groups(self):
        class Empty(FakeAdmin):
            def list_consumer_groups(self, request_timeout=None):
                return FakeFuture(types.SimpleNamespace(valid=[], errors=[]))
        result = kafka_consumer_groups(admin=Empty())
        self.assertEqual(result["group_count"], 0)
        self.assertEqual(result["rows"], [])


if __name__ == "__main__":
    unittest.main()
