# Phase 7 — StreamShield browser dashboard (first version)

**Status:** Dashboard implementation and unit tests committed; not yet executed or visually confirmed on the user's Mac.

## What is displayed

Run a local Streamlit app to inspect **historical synthetic datasets** and benchmark reports. Pages:

- **Overview:** PostgreSQL transaction, synthetic risk-flag, unique-account and category KPI cards/charts, plus benchmark performance.
- **Transactions & Risk:** historical UTC daily transaction counts, merchant-category volumes/flag rates, and overlapping rule-based signals.
- **Performance:** separate 100K and 1M Kafka producer throughput and Spark query processing throughput, from saved benchmark JSON.
- **Pipeline Architecture:** a summary of architecture, data boundaries and limitations.

The PostgreSQL data is the previously backfilled Phase 4/5 demo snapshot. **It is NOT the 100K or 1M benchmark output**. These larger benchmark results appear only as aggregates from saved JSON files. This is not a real-time Kafka lag dashboard yet.

**Read-only protection:** Dashboard SQL connection uses a read-only default transaction option; no INSERT/UPDATE/DELETE, Docker operations, Spark jobs or Kafka consumers are run by the dashboard.

## Local Mac setup

From the existing project directory:

```bash
cd ~/Documents/streamshield-realtime-fraud-platform
git pull
source .venv-spark/bin/activate
python -m pip install -e '.[dashboard]'
python -m unittest discover -s tests -v
docker compose ps
```

If PostgreSQL is not running, start the **existing** containers without deleting volumes:

```bash
docker compose up -d
```

Preferred PostgreSQL source is the previously verified UTC-corrected database named `streamshield_utc`. Its existing data should be present; do not recreate/reset it. If your database is named differently, explicitly set `STREAMSHIELD_PG_DSN` to your local address and database.

The default connection string is for **local development only** and must not be used on a publicly exposed deployment.

## Start the dashboard

```bash
streamlit run dashboard/app.py
```

Streamlit normally opens the page at:

http://localhost:8501

If your browser doesn't open by itself, copy that URL into your browser. Leave the Terminal process running while viewing the dashboard. Use `Ctrl+C` in that Terminal to stop it safely; this does not delete any data.

## Data freshness and limitations

- PostgreSQL charts represent static/durable analytics tables updated only when the **separate** batch/backfill job is run. They do not reflect new Kafka events automatically.
- Spark benchmark performance is loaded from `outputs/spark_benchmark_100k_v2/benchmark_result.json` and `outputs/spark_benchmark_1m_v1/benchmark_result.json`.
- The 1M Kafka producer report is loaded from `outputs/spark_benchmark_1m_v1/producer_result.json`. The 100K Kafka producer measurement (0.987 s / 101,282.7 acknowledged events/s) is a documented, user-verified terminal value embedded as a fallback in the data loader because the original producer did not save a JSON file.
- The dashboard does not itself verify integrity or measure new throughput; those checks are performed by the earlier separate read-only verifiers.
- All risk flags are synthetic illustrative rules, **not actual fraud detection accuracy**.
- Live Kafka broker health, consumer-group lag, in-flight transaction timing, p50/p95/p99 end-to-end latency, and on-call alerts are not yet implemented.

## Troubleshooting

- **Could not connect to PostgreSQL:** verify `docker compose ps` and the `streamshield_utc` database. The Performance and Architecture pages work without PostgreSQL.
- **No benchmark bars:** verify the local saved JSON filenames exist under `outputs/`. The dashboard never invents benchmark metrics.
- **Missing dependency:** activate `.venv-spark` and install `-e '.[dashboard]'`.
- **Port 8501 already taken:** use `streamlit run dashboard/app.py --server.port 8502` and browse to `http://localhost:8502`.
- **Streamlit email prompt:** you can press Enter to skip providing an email.
- **Don't deploy to public internet** with embedded development database credentials or local Docker ports exposed.

## Acceptance checklist (not yet user-verified)

1. Unit tests pass.
2. Browser page opens at localhost:8501.
3. Performance shows both 100K and 1M, with 11,379.5 and 51,459.2 Spark events/sec.
4. PostgreSQL Overview shows the previously loaded demonstration dataset without mixing in benchmark transactions.
5. Switching pages works, and Refresh data doesn't mutate any source.
