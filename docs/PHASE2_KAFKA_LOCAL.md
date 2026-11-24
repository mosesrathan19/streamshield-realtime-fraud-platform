# Phase 2 — Local Kafka + Docker (macOS)

**Status:** Code and configuration committed; Kafka runtime not yet verified on your machine.

Prerequisites: Docker Desktop running, Python 3.10+, 4–6 GB free RAM recommended. No AWS account required.

## Start broker

```bash
git pull
docker compose up -d
docker compose ps
```

Wait until `kafka` says `healthy` (can take about a minute). If it fails, inspect `docker compose logs kafka`.

## Install client and initialize topics

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[kafka]'
python -m streamshield.kafka_client init-topics
```

Topics: `streamshield.transactions.v1` (6 partitions), `streamshield.alerts.v1` (3 partitions), `streamshield.invalid.v1` (3 partitions). Phase 2 consumer currently stores alerts/quarantined offsets in SQLite; **it does not publish to the alerts or DLQ topics yet**. Those are reserved for later phases.

## Send and receive 10,000 synthetic transactions

**Terminal A** (consume first to make processing visible):

```bash
source .venv/bin/activate
python -m streamshield.kafka_client consume --max-messages 10000
```

**Terminal B** (run in the same repository):

```bash
source .venv/bin/activate
python -m streamshield.kafka_client produce --count 10000
```

Expect `attempted=acknowledged=10000`, `failed=0`. The consumer should show 10,000 processed records when using a new database and consumer group. Kafka messages persist even when you consume after producing, because the broker retains them.

**Replay demonstration**: Produce the same IDs again with the same `--count` and `--start`, then use a *new consumer group* but the *same SQLite database*, and increase `--max-messages` to cover all retained messages. It should register duplicates rather than create duplicate alerts. Topic retention and group offset semantics affect what is consumed.

### Design caveats

- Local single-node KRaft Kafka, replication factor 1, plaintext listener bound to `127.0.0.1`. **Not production HA or secure internet-facing configuration.**
- Messages keyed by `account_id` preserve per-partition Kafka order, not global order.
- Durable SQLite transaction is committed before the Kafka offset is committed. Crashes between those actions can cause redelivery, while the primary key prevents duplicate persisted effects.
- The consumer uses a synchronous offset commit per record to favor clarity over throughput. Benchmark results from Phase 1 do **not** apply to Kafka.
- For failure tests or multi-consumer scaling, use a new group ID and/or remove the demo DB; avoid accidentally comparing previously committed offsets to new event counts.
- Phase 3 will introduce Spark Structured Streaming and event-time aggregation.

## Stop / clear demo

```bash
docker compose down
```

To **erase Kafka history and reset broker storage** (irreversible):

```bash
docker compose down -v
```

Never run `down -v` if you want to retain generated Kafka topic data.
