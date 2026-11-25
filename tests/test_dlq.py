"""Phase 5D fixture and quarantine-verifier unit tests without Spark/Kafka."""
import unittest
from streamshield.dlq_fixture import fixture
from streamshield.dlq_verify import verify


class DLQTests(unittest.TestCase):
    def test_fixture_contains_six_events_with_unparseable_json(self):
        items = fixture()
        self.assertEqual(len(items), 6)
        self.assertEqual(items[-1], b"{bad JSON")

    def test_expected_quarantine_reconciliation(self):
        accepted = [
            dict(source_topic="test", source_partition=0, source_offset=i,
                 event_id=f"dlq_good_{i+1}") for i in range(2)
        ]
        rejected = [
            dict(source_topic="test", source_partition=0, source_offset=2,
                 event_id="dlq_bad_amount", reject_reason="invalid_amount"),
            dict(source_topic="test", source_partition=0, source_offset=3,
                 event_id="dlq_bad_account", reject_reason="missing_account_id"),
            dict(source_topic="test", source_partition=0, source_offset=4,
                 event_id="dlq_bad_time", reject_reason="invalid_event_time"),
            dict(source_topic="test", source_partition=0, source_offset=5,
                 event_id=None, reject_reason="missing_or_malformed_event_id"),
        ]
        self.assertEqual(verify(accepted, rejected)["status"], "PASS")
        with self.assertRaises(AssertionError):
            verify(accepted, rejected[:-1])


if __name__ == "__main__":
    unittest.main()
