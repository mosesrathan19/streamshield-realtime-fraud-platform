"""Phase 5: fault-injection safety checks without requiring a live database."""
import unittest
from streamshield.postgres_load import crash_after_commit


class RecoveryTests(unittest.TestCase):
    def test_disabled_injection_never_crashes(self):
        for n in range(1, 5):
            crash_after_commit(n, 0)

    def test_injection_only_at_selected_commit_boundary(self):
        crash_after_commit(1, 2)
        with self.assertRaisesRegex(RuntimeError, "SIMULATED_FAILURE_AFTER_COMMIT"):
            crash_after_commit(2, 2)
        crash_after_commit(3, 2)


if __name__ == "__main__":
    unittest.main()
