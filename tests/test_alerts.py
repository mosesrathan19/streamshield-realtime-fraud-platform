"""Unit tests for Phase 12 read-only health alert classifications."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))
import alerts


class HealthAlertsTests(unittest.TestCase):
    def test_all_reachable_reports_present_is_ok(self):
        result = alerts.evaluate_health(
            [{"component": "Kafka broker TCP", "endpoint": "localhost:9092", "reachable": True}],
            [{"run": "1M", "report": "spark", "present": True}],
        )
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["counts"]["OK"], 2)

    def test_tcp_failure_critical_even_when_report_missing(self):
        result = alerts.evaluate_health(
            [{"component": "Kafka broker TCP", "endpoint": "localhost:9092", "reachable": False}],
            [{"run": "1M", "report": "spark", "present": False}],
        )
        self.assertEqual(result["status"], "CRITICAL")
        self.assertEqual(result["counts"]["WARNING"], 1)
        self.assertEqual(result["counts"]["CRITICAL"], 1)
        self.assertIn("TCP connection failed", result["checks"][0]["detail"])

    def test_missing_report_is_warning_not_pipeline_failure(self):
        result = alerts.evaluate_health(
            [{"component": "PostgreSQL TCP", "endpoint": "localhost:5433", "reachable": True}],
            [{"run": "100K", "report": "spark", "present": False}],
        )
        self.assertEqual(result["status"], "WARNING")
        self.assertEqual(result["checks"][1]["kind"], "saved_file_presence")

    def test_empty_observations_not_marked_healthy(self):
        result = alerts.evaluate_health([], [])
        self.assertEqual(result["status"], "WARNING")
        self.assertEqual(result["counts"], {"OK": 0, "WARNING": 0, "CRITICAL": 0})

    def test_once_exit_codes(self):
        for status, expected in (("OK", 0), ("WARNING", 1), ("CRITICAL", 2)):
            with self.subTest(status=status):
                with patch.object(alerts, "take_snapshot", return_value={"status": status}):
                    with patch("builtins.print"):
                        self.assertEqual(alerts.main([]), expected)

    def test_watch_interval_safety(self):
        with self.assertRaises(SystemExit) as exc:
            alerts.main(["--watch", "--interval", "1"])
        self.assertEqual(exc.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
