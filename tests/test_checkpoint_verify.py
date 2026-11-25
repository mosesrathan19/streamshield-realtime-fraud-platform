"""Pure-python checks for Phase 5 checkpoint assertions."""
import unittest
from streamshield.checkpoint_verify import check_metrics


class CheckpointChecks(unittest.TestCase):
    def test_correct_output(self):
        self.assertEqual(check_metrics(100, 100, 100)["status"], "PASS")

    def test_missing_records(self):
        with self.assertRaisesRegex(AssertionError, "expected 100"):
            check_metrics(99, 99, 100)

    def test_duplicates(self):
        with self.assertRaisesRegex(AssertionError, "duplicate event"):
            check_metrics(100, 99, 100)

    def test_bad_expected_count(self):
        with self.assertRaises(ValueError):
            check_metrics(100, 100, 0)


if __name__ == "__main__":
    unittest.main()
