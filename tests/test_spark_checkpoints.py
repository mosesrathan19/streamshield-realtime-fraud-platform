"""Offline read-only tests for saved Spark checkpoint offset/commit metadata."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))
from spark_checkpoints import inspect_checkpoint, _offsets


def write_checkpoint(root: Path, batch: int, offsets: dict, *, committed=True):
    folder = root / "offsets"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / str(batch)).write_text(
        'v1\n{"batchWatermarkMs":0}\n' + json.dumps(offsets) + "\n",
        encoding="utf-8",
    )
    if committed:
        (root / "commits").mkdir(parents=True, exist_ok=True)
        (root / "commits" / str(batch)).write_text('v1\n{}\n', encoding="utf-8")


class CheckpointTests(unittest.TestCase):
    def test_missing_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            result = inspect_checkpoint(Path(directory) / "nonexistent")
        self.assertFalse(result["checkpoint_present"])
        self.assertEqual(result["committed_batches_total"], 0)

    def test_only_committed_batches_and_offset_differences(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_checkpoint(root, 0, {"a": {"0": 10, "1": 20}})
            write_checkpoint(root, 1, {"a": {"0": 14, "1": 27}})
            write_checkpoint(root, 2, {"a": {"0": 100, "1": 100}}, committed=False)
            result = inspect_checkpoint(root)
        self.assertEqual(result["offset_log_files"], 3)
        self.assertEqual(result["commit_log_files"], 2)
        self.assertEqual(result["committed_batches_total"], 2)
        self.assertEqual([r["batch"] for r in result["rows"]], [0, 1])
        self.assertIsNone(result["rows"][0]["offset_advance_from_prior_visible_batch"])
        self.assertEqual(result["rows"][1]["offset_advance_from_prior_visible_batch"], 11)

    def test_regressing_offsets_are_not_reported_as_negative_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_checkpoint(root, 0, {"a": {"0": 10}})
            write_checkpoint(root, 1, {"a": {"0": 5}})
            result = inspect_checkpoint(root)
        self.assertIsNone(result["rows"][1]["offset_advance_from_prior_visible_batch"])

    def test_invalid_source_offset_metadata_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "0"
            path.write_text('v1\n{}\n{"a":{"0":-1}}\n', encoding="utf-8")
            with self.assertRaises(ValueError):
                _offsets(path)

    def test_nonlog_files_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_checkpoint(root, 0, {"a": {"0": 1}})
            (root / "offsets" / ".0.crc").write_text("crc", encoding="utf-8")
            (root / "offsets" / "metadata").write_text("metadata", encoding="utf-8")
            result = inspect_checkpoint(root)
        self.assertEqual(result["offset_log_files"], 1)


if __name__ == "__main__":
    unittest.main()
