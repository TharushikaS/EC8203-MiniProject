# Report guide (8-15 pages): rubric → content → evidence

This maps every marking criterion to the report section that should address it and to the place in
the repository where the evidence lives, so the report can be split across team members.

| Rubric (marks) | Report section | Evidence in this repo |
|---|---|---|
| Architecture decision (20) | §2 Architecture decision: Lambda vs Kappa | `docs/architecture.md` §1-5 (requirements table, justification, Kappa comparison table, accepted trade-offs); reconciliation numbers from `speed_batch_reconciliation`; the recompute run in `batch_runs` (`run_type = recompute`, `tariff_version = 2`) |
| Tech stack (10) | §4 Technology stack | `docs/tech_stack.md` (each tool tied to a use-case constraint + alternatives) |
| Ingestion (15) | §5.1 Data sources & ingestion | `simulators/meter_stream.py` (keying, idempotent producer, fault injection), `simulators/daily_tariff_drop.py` (atomic drop, late files, bad rows), `infra/kafka/create-topics.sh`, `batch/reference_loader.py` (validation, quarantine, carry-forward, versions) |
| Processing (15) | §5.2 Speed layer, §5.3 Batch layer | `speed/transforms.py` (shared rules), `speed/streaming_job.py` (watermark, dedup, windows), `batch/batch_billing.py` (exact dedup, broadcast join, `mapInPandas` pricing), `common/billing.py` |
| Storage & serving (10) | §5.4 Serving layer | `infra/postgres/init/02_smartgrid_schema.sql`, `serving/api.py`, `serving/lambda_merge.py`, Grafana dashboards, the daily HTML report |
| Observability (10) | §6 Observability | `docs/observability.md` (metric catalogue, 14 alert rules, tracing, verified failure drills), Pipeline Health dashboard |
| Report quality (15) | whole report | diagrams from `docs/architecture.md` / README (Mermaid; export to PNG via mermaid.live), screenshots (list in `docs/demo_guide.md`) |
| Code quality (5) | §9 Reproducibility | README quick start, `.env.example`, Docker Compose, 51 pytest tests (`docker compose --profile test run --rm tests`) |

## Suggested outline

1. **Introduction & use case** (0.5 p): Smart Grid scenario, business question, the two sources.
2. **Requirements** (1 p): the R1-R7 table (latency, correctness, replay per output).
3. **Architecture decision** (2-2.5 p): Lambda choice, the Kappa rejection table, trade-offs accepted and how they were mitigated (shared transforms + single pricing function + test).
4. **Architecture diagrams** (1 p): overall Lambda diagram; data-flow per layer; observability diagram.
5. **Technology stack** (1.5 p): the table from `docs/tech_stack.md`, trimmed.
6. **Implementation** (2.5 p): ingestion (topic design, faults), speed layer (watermark, windows, dedup, idempotent sinks), batch layer (DAG, sensor, validation, recompute), serving (merge).
7. **Observability design** (1.5 p): what is measured, how, why; alert table; tracing; the failure drill as evidence.
8. **Results** (2 p): screenshots (dashboards, report, Airflow, Spark UI, API JSON), a table of measured numbers (see below).
9. **Limitations & production scale** (1 p): see below.
10. **Individual contributions** (short): `CONTRIBUTIONS.md`.

## Measured results to quote (reference run, 250 households)

Re-query these for your own final run.

```sql
-- per-day lineage
SELECT bill_date, run_type, raw_records, invalid_records, duplicates_removed, late_records,
       households_billed, round(total_billed::numeric) AS billed, tariff_version, round(duration_seconds::numeric,1) AS secs
FROM batch_runs ORDER BY finished_at;
-- speed vs batch drift
SELECT bill_date, grid_zone, round(drift_kwh::numeric,2), drift_pct, late_records FROM speed_batch_reconciliation ORDER BY 1,2;
```
(run with `docker compose exec postgres psql -U smartgrid -d smartgrid`)

| Metric | Value observed |
|---|---|
| Producer throughput | ~80 readings/s (250 meters × 1 per 3.1 s), 6 partitions evenly loaded |
| Raw readings per simulated day | ~24,200 (incl. ~1 % duplicates and ~0.5 % invalid) |
| Duplicates removed by batch per day | ~220-260 |
| Invalid readings rejected per day | ~90-100, across 5 reasons |
| Late readings recovered by batch per day | ~80-170 (depends on the outage rate) |
| Speed micro-batch duration | 1.5-4 s steady state (`grid_state`, 10 s trigger) |
| Producer → speed-layer latency | p50 ≈ 13 s, p95 ≈ 22 s |
| Batch job per simulated day | ~70 s including Spark start-up (local[2]) |
| Recompute of one day after tariff correction | ~85 s; bills changed from LKR 152,708 to 168,726 with tariff v2 (+10 % rate) |

## Limitations and what we would change at production scale

| Area | Demo simplification | Production approach |
|---|---|---|
| Kafka | single broker, replication factor 1 | ≥ 3 brokers, RF = 3, `min.insync.replicas = 2`, rack awareness; Schema Registry (Avro/Protobuf) instead of raw JSON |
| Spark | local mode inside containers | YARN / Kubernetes cluster, dynamic allocation; RocksDB state store (`STATE_STORE=rocksdb`) |
| Master dataset | Parquet on a Docker volume | S3 / HDFS with a table format (Delta / Iceberg) for ACID appends, time travel and compaction of small files |
| Raw archive semantics | at-least-once `foreachBatch` append (dedup in batch) | Delta/Iceberg idempotent writes (txn id = batch id) for exactly-once raw ingestion |
| Serving store | single PostgreSQL | read replicas, or a time-series DB (TimescaleDB) for speed views, partitioned bill tables |
| Orchestration | LocalExecutor, one scheduler | CeleryExecutor / KubernetesExecutor, HA schedulers, SLAs, data-aware scheduling (Datasets) |
| Batch source | file drop on a shared volume | SFTP/object-storage landing with checksums, or CDC from the billing DB |
| Time | compressed clock (1 day = 5 min) | real time; batch at a fixed hour after midnight with a larger grace period for late meters |
| Security | plaintext, demo passwords, anonymous Grafana viewer | TLS + SASL on Kafka, secrets manager, RBAC, network policies, PII handling for customer data |
| Observability | Prometheus + webhook to the API | Loki/ELK for logs, OpenTelemetry tracing, Alertmanager → PagerDuty/Slack, SLO dashboards |
| Code duplication risk | shared transforms module + one pricing function | same, plus contract tests on both layers in CI |

**Honest trade-offs to discuss:** the speed layer's figures for "today" are provisional and
under-count late data by design. The Lambda architecture carries more operational components than
Kappa would. The reference laptop (4 cores) is CPU-bound when the batch job and the stream run
together, which shows up as transient latency alerts. That is a sizing problem, not a design flaw,
and the observability stack detects it.
