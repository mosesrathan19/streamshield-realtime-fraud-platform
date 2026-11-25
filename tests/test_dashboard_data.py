"""Fast tests for dashboard report loading; no Kafka/DB/Streamlit needed."""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "dashboard" / "data.py"
spec = importlib.util.spec_from_file_location("streamshield_dashboard_data", MODULE_PATH)
data = importlib.util.module_from_spec(spec)
spec.loader.exec_module(data)


class DashboardDataTests(unittest.TestCase):
    def test_missing_reports_are_safe(self):
        self.assertIsNone(data.load_json(None))
        self.assertIsNone(data.load_json(Path("/unlikely_streamshield_missing_report.json")))
        with patch.object(data, "BENCHMARKS", {"missing": {
                "spark": Path("/unlikely_streamshield_missing_report.json"),
                "producer": None}}):
            self.assertEqual(data.benchmarks(), [])

    def test_aggregate_report_is_separate_from_db(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "benchmark_result.json"
            p.write_text(json.dumps({
                "source_topic": "isolated.test",
                "persisted_events": 100,
                "distinct_event_ids": 100,
                "synthetic_risk_flags": 2,
                "spark_query_elapsed_seconds": 4,
                "persisted_events_per_query_second": 25,
            }))
            with patch.object(data, "BENCHMARKS", {"trial": {"spark": p, "producer": None}}):
                result = data.benchmarks()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["transactions"], 100)
            self.assertEqual(result[0]["spark_events_per_sec"], 25)
            self.assertIsNone(result[0]["producer_seconds"])

    def test_invalid_json_object_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "bad.json"
            p.write_text("[1,2,3]")
            with self.assertRaises(ValueError):
                data.load_json(p)


if __name__ == "__main__":
    unittest.main()
