"""Unit tests for local Kafka/Spark benchmark fixture and reconciliation."""
import unittest
from streamshield.stream_bench_fixture import (
    TOPIC, COUNT, START, PARTITIONS, expected_id, fixture_event,
)
from streamshield.stream_bench_verify import assess


def sample_partitions():
    counts = [COUNT // PARTITIONS] * PARTITIONS
    for p in range(COUNT % PARTITIONS):
        counts[p] += 1
    return [
        {"partition": p, "rows": n, "min_offset": 0, "max_offset": n - 1,
         "distinct_offsets": n}
        for p, n in enumerate(counts)
    ]


class StreamingBenchmarkTests(unittest.TestCase):
    def test_fixture_shape(self):
        self.assertEqual(TOPIC, "streamshield.benchmark_100k.v1")
        self.assertEqual(expected_id(0), f"evt_{START:012d}")
        self.assertEqual(fixture_event(COUNT - 1)["event_id"],
                         expected_id(COUNT - 1))
        with self.assertRaises(ValueError):
            fixture_event(COUNT)

    def test_reconciliation_accepts_contiguous_complete_offsets(self):
        self.assertEqual(assess(COUNT, COUNT, 0, sample_partitions())["status"], "PASS")

    def test_reconciliation_rejects_missing_rows(self):
        self.assertEqual(assess(COUNT - 1, COUNT - 1, 0,
                                sample_partitions())["status"], "FAIL")

    def test_reconciliation_rejects_bad_ids_and_duplicate_offsets(self):
        self.assertEqual(assess(COUNT, COUNT, 1, sample_partitions())["status"], "FAIL")
        partitions = sample_partitions()
        partitions[0]["distinct_offsets"] -= 1
        self.assertEqual(assess(COUNT, COUNT, 0, partitions)["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
