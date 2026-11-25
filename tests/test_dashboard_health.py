"""Tests for the Phase 9 read-only diagnostics."""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))

from health import tcp_reachable, dependency_checks, report_checks


class HealthTests(unittest.TestCase):
    def test_tcp_unreachable_local_closed_port(self):
        # Patch connection, rather than relying on particular local ports.
        with patch("health.socket.create_connection", side_effect=OSError("closed")):
            self.assertFalse(tcp_reachable("127.0.0.1", 9092))

    def test_dependencies_are_read_only_tcp_checks(self):
        with patch("health.tcp_reachable", side_effect=[True, False]) as connect:
            result = dependency_checks(kafka_host="kafka", kafka_port=19092,
                                       postgres_host="postgres", postgres_port=5432)
        self.assertEqual(len(result), 2)
        self.assertTrue(result[0]["reachable"])
        self.assertFalse(result[1]["reachable"])
        self.assertEqual(result[0]["endpoint"], "kafka:19092")
        self.assertEqual(connect.call_count, 2)

    def test_report_check_ignores_unrecorded_100k_producer(self):
        results = report_checks()
        self.assertEqual(len(results), 3)
        self.assertEqual({(r["run"], r["report"]) for r in results},
                         {("100K", "spark"), ("1M", "spark"), ("1M", "producer")})
        self.assertTrue(all(isinstance(r["present"], bool) for r in results))


if __name__ == "__main__":
    unittest.main()
