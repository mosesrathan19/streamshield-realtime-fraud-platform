# Phase 12 — Read-Only Local Health Checks and Alerts

**Status: Locally verified November 25, 2026 (November 26 UTC): 62 unit tests passed, dashboard browser check passed, and one-shot CLI JSON checks passed.**

## Scope

This phase adds a **Health Alerts** page to Streamlit and a local CLI at `dashboard/alerts.py`. It reuses the Phase 9 diagnostics:

- Kafka broker **TCP reachability** (not Kafka protocol readiness)
- PostgreSQL **TCP reachability** (not verified SQL query correctness)
- Existing 100K and 1M benchmark **aggregate JSON report presence** (not freshness or integrity)

The checker does **not** infer Spark is down when an `availableNow` batch query has finished. It also deliberately does **not** label consumer-group lag as a production incident: many saved groups belong to completed local experiments, and Spark tracks offsets in its checkpoint.

## Local classification

| Observed condition | Label | Interpretation |
|---|---|---|
| All checked TCP ports reachable and expected report files present | OK | Only these limited checks passed |
| At least one expected aggregate report missing | WARNING | File missing or not mounted; not proof of failed pipeline |
| Kafka or PostgreSQL TCP connection failed | CRITICAL | TCP connectivity problem; does not prove broker/database itself failed |
| Diagnostic throws an error | UNKNOWN | Cannot determine health |

All statuses are **local observations**, not production severity guarantees. The dashboard's values are point-in-time snapshots, refreshed by user interaction.

## Commands: local verification

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
python -m unittest discover -s tests -v
docker compose --profile dashboard up -d --build
```

Open `http://localhost:8501` and choose **Health Alerts**.

### Optional: one-shot JSON health check

Run from the project root in the Mac Terminal:

```bash
python dashboard/alerts.py
```

Exit code `0` means OK, `1` WARNING, `2` CRITICAL, and `3` UNKNOWN (observation could not be completed). Note the CLI's TCP defaults are **host-side** `127.0.0.1:9092` and `127.0.0.1:5433`; the Docker dashboard uses service DNS names and internal ports.

### Optional: repeated foreground checks

```bash
python dashboard/alerts.py --watch --interval 60
```

This prints a JSON snapshot every 60 seconds **only while the command remains running**. Press `Ctrl+C` to stop. There is **no scheduler, daemon, push notification, email, webhook, SMS, or automatic repair**. The polling interval must be at least five seconds.

### Safety / exclusions

- No database mutation, Kafka consumption, group commits, event publication, or Spark/Parquet writes
- No checkpoint changes, benchmark replays, Docker restarts, or auto-remediation
- Never run `docker compose down -v` for this feature
- Historical Spark records and saved reports are not live workload SLOs

The CLI uses the existing port checks; a stopped Kafka or PostgreSQL container may appear as `CRITICAL` even when shutdown is intentional. During local development, this is a **connectivity notice**, not a production incident.

## Acceptance

1. All unit tests pass, including six new Phase 12 alert cases.
2. Dashboard **Health Alerts** page loads and displays the result rows with no exception.
3. One-shot CLI returns JSON with `status`, `counts`, `checks`, and `checked_at_utc`.
4. Running repeated checks is optional, not a prerequisite for verification.

Report the actual observed status; do not call a snapshot healthy solely because a dashboard was able to render.

## Observed local verification

On November 25, 2026 Pacific time, the user confirmed **62 tests passed** (no failures or errors) and supplied a browser screenshot of the **Health Alerts** page. At **2026-11-26T00:54:43Z**, the dashboard showed **overall OK**, **5 checks OK**, **0 warnings**, **0 critical connectivity failures**. The table showed Kafka broker TCP reachable at `kafka:19092`, PostgreSQL TCP reachable at `postgres:5432`, and three reports present: 100K Spark, 1M Spark, 1M producer. This validates the local browser presentation and the five checks; it does not demonstrate active Spark health, alert delivery, or the CLI. The user subsequently shared the one-shot CLI JSON results: Kafka broker TCP at `127.0.0.1:9092` and PostgreSQL TCP at `127.0.0.1:5433` each returned `OK`; all three saved benchmark report presence checks returned `OK`; counts were `OK: 5`, `WARNING: 0`, `CRITICAL: 0`. The supplied excerpt did not show the top-level `status` or exit code, so neither is independently asserted here.
