"""Phase 11: normalize Spark StreamingQueryProgress from a fresh isolated run.

Metrics describe Spark input batches, not Kafka-to-user latency or accepted
output rows. No changes to existing Spark checkpoints or benchmark reports.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json


def normalize_progress(events: list[dict]) -> list[dict]:
    """Keep selected numeric metrics with their Spark-defined meanings."""
    normalized = []
    seen = set()
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("Spark progress event must be an object")
        batch = int(event["batchId"])
        if batch in seen:
            raise ValueError("Duplicate Spark progress batch ID")
        seen.add(batch)
        duration = event.get("durationMs") or {}
        if not isinstance(duration, dict):
            raise ValueError("Invalid durationMs map")
        normalized.append({
            "batch_id": batch,
            "timestamp_utc": str(event.get("timestamp", "")),
            "num_input_rows": int(event["numInputRows"]),
            "input_rows_per_second": float(event["inputRowsPerSecond"]),
            "processed_rows_per_second": float(event["processedRowsPerSecond"]),
            "trigger_execution_ms": (
                int(duration["triggerExecution"]) if duration.get("triggerExecution") is not None
                else None
            ),
            "add_batch_ms": (
                int(duration["addBatch"]) if duration.get("addBatch") is not None
                else None
            ),
        })
    return sorted(normalized, key=lambda item: item["batch_id"])


def progress_report(events: list[dict], *, topic: str) -> dict:
    rows = normalize_progress(events)
    return {
        "schema_version": 1,
        "status": "completed_query_progress",
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_topic": topic,
        "progress_rows": len(rows),
        "total_input_rows_across_batches": sum(x["num_input_rows"] for x in rows),
        "scope": (
            "Spark StreamingQueryProgress collected from this run's completed "
            "availableNow query; batch input counts and Spark-reported processing rates."
        ),
        "not_measured": [
            "end-to-end Kafka-to-Parquet per-event latency",
            "p95/p99 transaction latency",
            "steady-state production throughput",
        ],
        "batches": rows,
    }


def load_progress(path) -> dict | None:
    """Read an aggregate progress report only; never load raw Kafka events."""
    from pathlib import Path
    path = Path(path)
    if not path.is_file():
        return None
    with path.open(encoding="utf-8") as fh:
        result = json.load(fh)
    if result.get("schema_version") != 1 or result.get("status") != "completed_query_progress":
        raise ValueError("Unrecognized Spark progress report")
    if not isinstance(result.get("batches"), list):
        raise ValueError("Missing Spark batch progress")
    result["batches"] = normalize_progress([
        {
            "batchId": x["batch_id"],
            "timestamp": x["timestamp_utc"],
            "numInputRows": x["num_input_rows"],
            "inputRowsPerSecond": x["input_rows_per_second"],
            "processedRowsPerSecond": x["processed_rows_per_second"],
            "durationMs": {
                "triggerExecution": x["trigger_execution_ms"],
                "addBatch": x["add_batch_ms"],
            },
        }
        for x in result["batches"]
    ])
    return result
