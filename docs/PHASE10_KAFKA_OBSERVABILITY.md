# Phase 10A — Kafka Topic Offset Observability

**Status:** Implemented in GitHub; awaiting local unit tests, Docker browser verification, and broker output inspection.

## What was added

The optional Docker dashboard now has a **Kafka Topics** page. It uses Kafka's `AdminClient` API only to retrieve existing `streamshield.*` topic metadata and earliest/latest offsets for each partition.

It displays:

- Number of matching Kafka topics
- Number of partitions inspected
- Earliest retained log offset, latest log offset and their difference per partition
- Sum of the retained offset spans (an offset range size, **not an exact count of messages**)

The observer **does not** subscribe to Kafka, create consumer groups, read payloads, write commits, or publish events. There is **no destructive operation** and no changes to existing Spark checkpoints.

## Critical data interpretation

The retained offset span `latest - earliest` **is not Kafka consumer lag** and may differ from exact message counts due to log retention, compaction, or gaps. Kafka reports end offsets even when a Spark job is stopped; these numbers do **not** establish whether Spark has processed its inputs.

No consumer-group lag, Spark last processed offsets, Spark microbatch latency, or live application metrics are implemented in Phase 10A.

## Run on your Mac

From the existing repository root:

```bash
git pull
source .venv-spark/bin/activate
python -m unittest discover -s tests -v
docker compose --profile dashboard up -d --build
```

Open **http://localhost:8501** → select **Kafka Topics**. Existing Kafka and PostgreSQL services are reused; outputs are mounted read-only in the dashboard container.

Confirm that you see one or more `streamshield.*` topics with partition offset rows. With local Kafka 24-hour retention, older synthetic benchmark messages may have aged out; values can legitimately differ from earlier benchmark sizes.

The Docker Compose service sets `STREAMSHIELD_KAFKA_BOOTSTRAP=kafka:19092`; the host-mode default is `127.0.0.1:9092`. The dashboard optional dependency now includes `confluent-kafka`.

**Safe stop, dashboard only:**

```bash
docker compose --profile dashboard stop dashboard
```

Do not run volume-removal or topic-deletion commands.

## Troubleshooting

```bash
docker compose --profile dashboard logs --tail=80 dashboard
docker compose --profile dashboard ps
```

If the Kafka Topics page says inspection unavailable, note the error text and confirm Kafka is healthy before changing settings. No auto-created topic is expected since the admin client calls `list_topics()` without a topic name.

## Next possible improvement

A future Phase 10B can inspect **specific existing Kafka consumer groups** and their committed offsets when those groups exist, without joining the group. Spark source progress must be tracked independently through Spark's own progress/checkpoint metrics.
