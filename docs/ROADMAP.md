# Engineering acceptance criteria

## Phase 2 — Kafka
- Containerized single-node dev broker; health check; create transactions/alerts/DLQ topics.
- Key by account_id; document ordering guarantees within partitions.
- Batch producer with retry/backoff; consumer lag observation.

## Phase 3 — Spark streaming
- Read Kafka using version-matched Spark Kafka connector.
- Parse versioned event contract; separate invalid payloads without losing offsets.
- 10-minute event-time risk aggregation; watermark and lateness policy.
- Deduplicate with bounded state and document limitations of watermarks.

## Phase 4 — Storage and analytics
- PostgreSQL tables partitioned/indexed for transaction and alert reporting.
- Upserts under replay with stable event keys and idempotent micro-batch writes.
- SQL models: transaction rate, fraud-signal rate, merchant/category statistics.

## Phase 5 — Reliability
- SIGKILL/restart stream processor; verify no missing/duplicate persisted effects.
- Inject delayed, out-of-order, malformed and replayed events.
- Reconciliation: produced = accepted + quarantined + duplicates (define units clearly).

## Phase 6 — Scale
- 1M and 5M event fixtures generated from seeds, not checked into Git.
- Measure producer throughput, consumer throughput, end-to-end p50/p95/p99 latency and lag, checkpoint size, resource use.
- Publish exact machine/configuration, observed results and methodology.

## Phase 7 — Operations
- Metrics dashboards, alerts on lag/invalid share/error rate; actionable runbook.
- Local-dev security guidance and deployment threat model.
