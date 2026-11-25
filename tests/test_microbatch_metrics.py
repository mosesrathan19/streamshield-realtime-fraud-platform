"""Offline tests for Phase 11 Spark micro-batch progress parsing and safeguards."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from streamshield.microbatch_metrics import normalize_progress, progress_report, load_progress
from streamshield.microbatch_spark import preflight


def sample(batch: int, rows: int = 500) -> dict:
    return {
        "batchId": batch,
        "timestamp": "2026-11-25T12:00:00.000Z",
        "numInputRows": rows,
        "inputRowsPerSecond": 123.5,
        "processedRowsPerSecond": 456.25,
        "durationMs": {"triggerExecution": 1400, "addBatch": 1000},
    }


class MicrobatchTests(unittest.TestCase):
    def test_normalize_numeric_rates_and_durations(self):
        rows = normalize_progress([sample(1), sample(0, 300)])
        self.assertEqual([r["batch_id"] for r in rows], [0, 1])
        self.assertEqual(rows[0]["num_input_rows"], 300)
        self.assertEqual(rows[0]["trigger_execution_ms"], 1400)
        self.assertEqual(rows[0]["processed_rows_per_second"], 456.25)

    def test_duplicate_batch_is_rejected(self):
        with self.assertRaises(ValueError):
            normalize_progress([sample(0), sample(0)])

    def test_report_uses_actual_input_rows_not_persisted_row_count(self):
        report = progress_report([sample(0, 400), sample(1, 300)], topic="streamshield.fixture")
        self.assertEqual(report["total_input_rows_across_batches"], 700)
        self.assertEqual(report["progress_rows"], 2)
        self.assertEqual(report["status"], "completed_query_progress")

    def test_roundtrip_and_missing_report(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "progress.json"
            self.assertIsNone(load_progress(path))
            report = progress_report([sample(0)], topic="streamshield.fixture")
            path.write_text(json.dumps(report), encoding="utf-8")
            loaded = load_progress(path)
        self.assertEqual(loaded["batches"][0]["input_rows_per_second"], 123.5)

    def test_preflight_does_not_allow_existing_sink_or_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            preflight(root)
            (root / "checkpoints").mkdir()
            with self.assertRaises(RuntimeError):
                preflight(root)


if __name__ == "__main__":
    unittest.main()
