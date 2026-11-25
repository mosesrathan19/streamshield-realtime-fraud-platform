"""Read-only Spark Structured Streaming checkpoint inspection.

Only reads Spark's checkpoint offsets/ and commits/ text files. Does NOT use
Spark APIs, start a driver, change checkpoints, or infer live query status.
Only the committed offset log batch IDs are presented as completed batches.
Spark Kafka offset maps represent source log positions, not accepted record
counts or historical processing rates.
"""
from __future__ import annotations

import json
from pathlib import Path

from data import ROOT

CHECKPOINTS = {
    "Phase 3 accepted": ROOT / "outputs/spark_phase3/checkpoints/accepted",
    "100K v2 accepted": ROOT / "outputs/spark_benchmark_100k_v2/checkpoints/accepted",
    "1M v1 accepted": ROOT / "outputs/spark_benchmark_1m_v1/checkpoints/accepted",
}
MAX_BATCHES = 1000
MAX_LOG_SIZE = 2_000_000


def _batch_files(folder: Path) -> dict[int, Path]:
    if not folder.is_dir():
        return {}
    files = {}
    for path in folder.iterdir():
        if path.is_file() and path.name.isdecimal():
            if path.stat().st_size <= MAX_LOG_SIZE:
                files[int(path.name)] = path
    return files


def _offsets(path: Path) -> dict:
    """Read Spark offset log with version and metadata header."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 3 or lines[0] != "v1":
        raise ValueError("Not a recognized Spark v1 offset log")
    # First source (Kafka) follows the metadata JSON line.
    meta = json.loads(lines[1])
    if not isinstance(meta, dict):
        raise ValueError("Missing Spark offset metadata")
    source = json.loads(lines[2])
    if not isinstance(source, dict):
        raise ValueError("Kafka source offset map missing")
    result = {}
    for topic, parts in source.items():
        if not isinstance(parts, dict):
            raise ValueError("Invalid topic partition offset map")
        for part, offset in parts.items():
            if not str(part).isdecimal() or type(offset) is not int or offset < 0:
                raise ValueError("Invalid nonnegative Kafka end offset")
            result[f"{topic}/{part}"] = offset
    return result


def inspect_checkpoint(path: Path, *, max_batches: int = MAX_BATCHES) -> dict:
    """Return persisted committed batch summaries without mutating files.

    A commit log file marks successful sink completion for that batch.
    We present its corresponding source end-offset map only if both files
    are available. Differences between consecutive committed maps are
    source OFFSET advancements, not accepted output row counts.
    """
    root = Path(path)
    offset_files = _batch_files(root / "offsets")
    commit_files = _batch_files(root / "commits")
    committed = sorted(set(offset_files) & set(commit_files))
    selected = committed[-max_batches:] if max_batches > 0 else []
    rows = []
    previous = None
    for batch_id in selected:
        parsed = _offsets(offset_files[batch_id])
        if previous is None:
            delta = None
        else:
            delta = sum(parsed[key] - previous[key] for key in parsed if key in previous) if (
                parsed.keys() == previous.keys() and
                all(parsed[k] >= previous[k] for k in parsed)
            ) else None
        rows.append({
            "batch": batch_id,
            "source_partitions": len(parsed),
            "source_offset_end_total": sum(parsed.values()),
            "offset_advance_from_prior_visible_batch": delta,
        })
        previous = parsed
    return {
        "checkpoint_present": root.is_dir(),
        "offset_log_files": len(offset_files),
        "commit_log_files": len(commit_files),
        "committed_batches_total": len(committed),
        "batches_shown": len(rows),
        "rows": rows,
    }


def inspect_known_checkpoints() -> list[dict]:
    return [
        {"name": name, **inspect_checkpoint(path)}
        for name, path in CHECKPOINTS.items()
    ]
