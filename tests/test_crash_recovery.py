"""Pure-Python tests for isolated Spark crash fixture/reconciliation."""
import json
import unittest
from streamshield.crash_fixture import COUNT, START, TOPIC, payloads
from streamshield.crash_verify import verify


class CrashRecoveryChecks(unittest.TestCase):
    def test_fixture_is_isolated_and_deterministic(self):
        self.assertEqual(TOPIC, "streamshield.crash_test.v1")
        batch = payloads()
        self.assertEqual(len(batch), COUNT)
        self.assertEqual(json.loads(batch[0])["event_id"], f"evt_{START:012d}")
        self.assertEqual(json.loads(batch[-1])["event_id"],
                         f"evt_{START + COUNT - 1:012d}")

    def test_partial_and_complete_reconciliation(self):
        records = [(0, i, f"evt_{START + i:012d}") for i in range(COUNT)]
        self.assertEqual(verify(records[:50])["status"], "PASS")
        self.assertEqual(verify(records, full=True)["status"], "PASS")

    def test_reject_duplicates_and_missing_input(self):
        first = (0, 0, f"evt_{START:012d}")
        with self.assertRaises(AssertionError):
            verify([first, first])
        with self.assertRaises(AssertionError):
            verify([first], full=True)
        with self.assertRaises(AssertionError):
            verify([])


if __name__ == "__main__":
    unittest.main()
