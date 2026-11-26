"""Phase 12 local health alerts: conservative, read-only status classification.

These checks inspect TCP reachability and saved report file presence. They
cannot establish application readiness, live Spark activity, or end-to-end
pipeline health. Alerting is local-only: no email, webhook, Kafka consumption,
database writes, checkpoint edits, or automatic repairs.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import time

from health import dependency_checks, report_checks

SEVERITY_RANK = {"OK": 0, "WARNING": 1, "CRITICAL": 2}


def evaluate_health(dependencies: list[dict], reports: list[dict]) -> dict:
    """Deterministic health classifications from injected observation snapshots."""
    checks = []
    for dep in dependencies:
        name = str(dep["component"])
        address = str(dep["endpoint"])
        reachable = bool(dep["reachable"])
        checks.append({
            "check": name,
            "kind": "tcp_reachability",
            "status": "OK" if reachable else "CRITICAL",
            "detail": (
                f"TCP connection succeeded at {address}; application readiness unverified"
                if reachable else f"TCP connection failed at {address}; service status unknown"
            ),
        })
    for report in reports:
        run = str(report["run"])
        kind = str(report["report"])
        present = bool(report["present"])
        checks.append({
            "check": f"{run} {kind} saved report",
            "kind": "saved_file_presence",
            "status": "OK" if present else "WARNING",
            "detail": (
                "Saved report file exists (not validated for correctness or freshness)"
                if present else "Saved report file missing or not mounted"
            ),
        })
    overall = max(
        (entry["status"] for entry in checks),
        key=lambda state: SEVERITY_RANK[state],
        default="WARNING",
    )
    return {
        "status": overall,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
        "counts": {
            status: sum(x["status"] == status for x in checks)
            for status in ("OK", "WARNING", "CRITICAL")
        },
        "scope": "Read-only local TCP connectivity and saved aggregate report presence",
        "limitations": (
            "Not Kafka readiness, database SQL health, live Spark activity, "
            "consumer lag alerts, or an end-to-end SLA"
        ),
    }


def take_snapshot() -> dict:
    return evaluate_health(dependency_checks(), report_checks())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only StreamShield local health checks")
    parser.add_argument(
        "--watch", action="store_true",
        help="Opt-in foreground repeated checks (Ctrl+C to stop); no background scheduler"
    )
    parser.add_argument("--interval", type=float, default=60.0,
                        help="Seconds between foreground checks, minimum 5 (default 60)")
    args = parser.parse_args(argv)
    if args.watch and args.interval < 5:
        parser.error("--interval must be at least 5 seconds")

    try:
        while True:
            try:
                result = take_snapshot()
                print(json.dumps(result, indent=2), flush=True)
                code = 2 if result["status"] == "CRITICAL" else (
                    1 if result["status"] == "WARNING" else 0
                )
            except (OSError, ValueError, RuntimeError) as exc:
                # Unable to measure health is not "healthy".
                print(json.dumps({"status": "UNKNOWN", "error": str(exc)}), flush=True)
                code = 3
            if not args.watch:
                return code
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
