"""Read-only local dependency diagnostics for StreamShield.

No Kafka consumer group is created and no checkpoint, output, or database is mutated.
These checks show reachability and report presence, not throughput or consumer lag.
"""
from __future__ import annotations

import os
from pathlib import Path
import socket

from data import BENCHMARKS


def tcp_reachable(host: str, port: int, timeout: float = 1.5) -> bool:
    """Return True when a TCP connection to a local dependency succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, OverflowError, ValueError):
        return False


def dependency_checks(
    *,
    kafka_host: str | None = None,
    kafka_port: int | None = None,
    postgres_host: str | None = None,
    postgres_port: int | None = None,
) -> list[dict]:
    """Simple TCP reachability, not a Kafka/SQL application health guarantee."""
    k_host = kafka_host or os.getenv("STREAMSHIELD_KAFKA_HOST", "127.0.0.1")
    k_port = kafka_port if kafka_port is not None else int(os.getenv("STREAMSHIELD_KAFKA_PORT", "9092"))
    p_host = postgres_host or os.getenv("STREAMSHIELD_PG_HOST", "127.0.0.1")
    p_port = postgres_port if postgres_port is not None else int(os.getenv("STREAMSHIELD_PG_PORT", "5433"))
    return [
        {"component": "Kafka broker TCP", "endpoint": f"{k_host}:{k_port}",
         "reachable": tcp_reachable(k_host, k_port)},
        {"component": "PostgreSQL TCP", "endpoint": f"{p_host}:{p_port}",
         "reachable": tcp_reachable(p_host, p_port)},
    ]


def report_checks() -> list[dict]:
    """Check only local benchmark aggregate reports; never read raw event payloads."""
    reports = []
    for name, paths in BENCHMARKS.items():
        for kind, path in paths.items():
            if path is None:
                continue  # 100K producer result was recorded in terminal, not JSON
            reports.append({
                "run": name, "report": kind,
                "present": Path(path).is_file(),
                "path": str(path),
            })
    return reports
