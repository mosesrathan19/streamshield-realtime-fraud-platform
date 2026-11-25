# Phase 9 — Optional Docker Dashboard and Local Health Diagnostics

**Status:** Verified on macOS, November 25, 2026: Docker image built, Kafka and PostgreSQL healthy, Streamlit System Health page rendered in browser, both services reachable over TCP, all three saved benchmark reports present, and the local unit test suite passed (reported by the user).

## What was added

- A fifth Streamlit page, **System Health**, with point-in-time TCP reachability for Kafka and PostgreSQL and a listing of saved benchmark aggregate JSON reports.
- An **opt-in** Docker Compose `dashboard` profile, separate from existing Kafka and PostgreSQL services.
- A dashboard-only Python image; no Spark worker or Kafka consumer runs in that image.
- A read-only bind mount for `outputs/`, keeping existing Spark Parquet, checkpoints, benchmark files, and SQLite artifacts unchanged.
- New unit tests for dependency diagnostics.

These checks are intentionally limited. TCP reachability is **not** consumer lag, broker readiness, SQL query correctness, throughput, crash detection, alert delivery or latency monitoring. The existing PostgreSQL dashboard snapshot is **batch backfilled**, not continuously streamed.

## Start the dashboard in Docker

Run from the repository root, after pulling changes:

```bash
git pull
docker compose --profile dashboard up -d --build
docker compose --profile dashboard ps
```

Open **http://localhost:8501** and select **System Health** in the sidebar. Check the two TCP results and the benchmark report statuses. Visit the existing Overview and Performance pages to confirm that the prior charts display normally.

The container uses a development-only database DSN pointing to `postgres:5432/streamshield_utc`, the independently verified UTC demo database. If that database hasn't been created on a different machine, the Overview will report a PostgreSQL query error; see Phase 4 setup. The service does not create or overwrite it.

**Safe shutdown** (dashboard only):

```bash
docker compose --profile dashboard stop dashboard
```

Do **not** use `docker compose down -v` or any commands that remove existing volumes, topics, outputs or checkpoints.

## Run the updated dashboard without Docker

The previous Mac workflow still works:

```bash
source .venv-spark/bin/activate
streamlit run dashboard/app.py
```

In this mode the health checks default to Kafka `127.0.0.1:9092` and PostgreSQL `127.0.0.1:5433`.

## Tests

```bash
python -m unittest discover -s tests -v
```

This includes three new unit tests in `tests/test_dashboard_health.py`. The previous 37 tests passed before Phase 9; three new health tests were added. The user confirmed the updated local test suite passed on November 25, 2026 (the exact count was not pasted).

## Troubleshooting

- Inspect dashboard logs: `docker compose --profile dashboard logs --tail=80 dashboard`
- Check existing container health: `docker compose ps`
- Check image build: `docker compose --profile dashboard build dashboard`
- Verify report files exist: `ls outputs/spark_benchmark_100k_v2/benchmark_result.json outputs/spark_benchmark_1m_v1/benchmark_result.json outputs/spark_benchmark_1m_v1/producer_result.json`

All data remains synthetic, and this configuration is intended for **local development only**. Port 8501 is bound to the loopback address.
