# Phase 10B — Kafka Consumer Group Lag (Read Only)

**Status:** Locally verified November 25, 2026. The user reported completing the local setup/tests; browser screenshot confirms the Docker Streamlit Consumer Groups page displayed two discovered and inspected groups, 12 group partitions with commits, and a point-in-time total comparable committed-offset lag of 200. These are Kafka group offsets, not Spark checkpoint progress.

## What this actually measures

For each existing Kafka consumer group with a committed offset on a `streamshield.*` topic partition:

`comparable lag = broker latest offset - committed consumer-group offset`

Lag is displayed **only** if the committed offset falls between Kafka's earliest retained and latest offsets. Unknown or missing committed offsets display `No committed offset`, not zero. Out-of-range offsets are reported as not comparable. Only the first 30 existing groups (alphabetically) are inspected; the page warns if more groups exist. This avoids unbounded polling.

The dashboard uses **Kafka AdminClient** to list consumer groups and read existing committed offsets. It **does not** create/join a consumer group, subscribe, consume payloads, commit/reset offsets, delete topics, or modify checkpoints.

**Important for Spark:** Spark Structured Streaming ordinarily stores Kafka source progress in its **Spark checkpoint** rather than Kafka consumer-group commits. Therefore no Kafka group commits may exist for the Spark jobs. In that case a blank lag table is an expected result, **not proof that Spark is caught up or idle**. Topic-offset spans from Phase 10A are not substituted as lag.

These are **point-in-time snapshots** on page load or manual refresh, not continuous monitoring, alerting, or latency percentiles.

## Verify locally

From project root on Mac:

```bash
git pull
source .venv-spark/bin/activate
python -m unittest discover -s tests -v
docker compose --profile dashboard up -d --build
```

Visit **http://localhost:8501**, select **Consumer Groups** in the sidebar. The page should either show real committed-group offset rows with comparable lag, or state no commits are available. This is a legitimate outcome for the existing Spark workloads.

You can check the broker independently from Terminal without creating any consumers:

```bash
docker exec streamshield-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

This lists groups only; do **not** run reset or delete commands.

When finished, safely stop **only** the dashboard:

```bash
docker compose --profile dashboard stop dashboard
```

All topics, Kafka data volumes, PostgreSQL snapshots, Spark outputs, and checkpoints are retained.

## Troubleshooting

```bash
docker compose --profile dashboard logs --tail=80 dashboard
docker compose --profile dashboard ps
```

If the page reports broker errors, share the visible error message. A successful Docker build alone does not establish that group metadata APIs are working.

## Future Phase 10C

To report actual Spark Structured Streaming processing progress, use explicit Spark query progress events or checkpoint metadata, with separate handling for active jobs versus archived benchmark runs. Do not claim Kafka committed-group lag reflects Spark progress.

## API documentation

[Confluent Python AdminClient consumer-group APIs](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
