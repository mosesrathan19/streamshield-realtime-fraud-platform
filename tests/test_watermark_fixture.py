"""Fast fixture/verifier tests without Spark or Kafka runtime."""
import unittest
from streamshield.watermark_fixture import fixture
from streamshield.watermark_verify import verify_ids


class WatermarkTests(unittest.TestCase):
    def test_fixture_times_and_unique_ids(self):
        early = fixture("seed")
        later = fixture("followup")
        self.assertEqual(len(early), 2)
        self.assertEqual(len(later), 4)
        self.assertEqual(len({x["event_id"] for x in early}), 2)
        self.assertEqual(later[-1]["event_id"], early[0]["event_id"])
        self.assertEqual(later[0]["event_time"], "2026-01-01T00:00:00+00:00")
        self.assertEqual(later[1]["event_time"], "2026-01-01T00:25:00+00:00")

    def test_seed_verifier(self):
        self.assertEqual(verify_ids(["wm_seed_1", "wm_seed_2"], "seed")["status"], "PASS")

    def test_followup_verifier(self):
        ids = ["wm_seed_1", "wm_seed_2", "wm_outorder", "wm_on_time"]
        self.assertTrue(verify_ids(ids, "followup")["late_event_absent"])

    def test_reject_late_and_duplicates(self):
        with self.assertRaises(AssertionError):
            verify_ids(["wm_seed_1", "wm_seed_2", "wm_too_late",
                        "wm_outorder", "wm_on_time"], "followup")
        with self.assertRaises(AssertionError):
            verify_ids(["wm_seed_1", "wm_seed_1"], "seed")


if __name__ == "__main__":
    unittest.main()
