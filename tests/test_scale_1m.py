"""Lightweight pure-Python checks for the isolated Phase 6D 1M setup."""
import unittest

from streamshield.events import event_at
from streamshield.scale_1m_fixture import COUNT, START, PARTITIONS, TOPIC, OUTPUT
from streamshield.scale_1m_verify import assess


class MillionEventBenchmarkTests(unittest.TestCase):
    def test_isolated_namespace_and_event_sequence(self):
        self.assertEqual(TOPIC, "streamshield.benchmark_1m.v1")
        self.assertEqual(str(OUTPUT), "outputs/spark_benchmark_1m_v1")
        self.assertEqual(COUNT, 1_000_000)
        self.assertEqual(event_at(START)["event_id"], "evt_000002000000")
        self.assertEqual(event_at(START + COUNT - 1)["event_id"], "evt_000002999999")

    def test_complete_offset_reconciliation(self):
        counts = [COUNT // PARTITIONS + (1 if p < COUNT % PARTITIONS else 0)
                  for p in range(PARTITIONS)]
        groups = [{"partition": p, "rows": n, "min_offset": 0,
                   "max_offset": n - 1, "distinct_offsets": n}
                  for p, n in enumerate(counts)]
        self.assertEqual(assess(COUNT, COUNT, 0, groups)["status"], "PASS")
        self.assertEqual(assess(COUNT - 1, COUNT - 1, 0, groups)["status"], "FAIL")
        self.assertEqual(assess(COUNT, COUNT, 1, groups)["status"], "FAIL")
        groups[0]["distinct_offsets"] -= 1
        self.assertEqual(assess(COUNT, COUNT, 0, groups)["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
