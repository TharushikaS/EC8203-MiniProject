# Demo guide (5-10 minute video or live demo)

Start the stack **at least 15 minutes before recording** (`docker compose up -d`) so that about
3 simulated days are already billed. Keep these tabs open: Grafana (3000), Airflow (8080),
API docs (8000/docs), Prometheus alerts (9090/alerts), Spark UI (4040), and a terminal.

| # | Time | Show | Say (key points) |
|---|---|---|---|
| 1 | 0:00-0:45 | README architecture diagram | Use case 3 and the business question. Why **Lambda**: bills must be exact, the tariff is a daily batch, corrections need recompute. Speed layer for live grid awareness. Simulated clock: 1 day = 5 min. |
| 2 | 0:45-1:45 | Terminal: `docker compose ps`, then `docker compose logs -f meter-simulator` | 250 meters → Kafka, keyed by household (6 partitions). JSON structured logs. Injected faults: duplicates, invalid readings, outages that upload late. |
| 3 | 1:45-2:30 | Kafka UI (optional) or Grafana *Pipeline Health* → "messages/s per partition" | Load spread over partitions. DLQ topic holds rejected readings. |
| 4 | 2:30-3:45 | Spark UI → Structured Streaming tab; Grafana *Live Grid Operations* | Two queries: `raw_ingest` (master dataset + quality) and `grid_state` (watermark 2 h, dedup, 1-h windows). Live load, solar and renewable share by zone; LOW_RENEWABLE alerts on cloudy days. |
| 5 | 3:45-4:30 | Browser: `localhost:8000/api/v1/realtime/zones`, `/api/v1/households/HH-00001/bill-estimate` | Real-time API. The bill estimate merges confirmed batch days with the provisional speed-layer today, each line labelled `source`. |
| 6 | 4:30-5:45 | Airflow → `smartgrid_daily_batch` graph + one run's logs | Sensor waits for the tariff file (sometimes late) → validation/quarantine → Spark batch job (dedup, tariff join, pricing) → reconcile → report → metrics. |
| 7 | 5:45-6:45 | `output/reports/<day>/daily_report_<day>.html`, Grafana *Daily Billing* | The consolidated report answers the business question: zone renewable share, hour-of-day profile, bills, alerts, data-quality lineage. |
| 8 | 6:45-7:30 | Billing dashboard → "Speed vs batch" panel | **Why the batch layer exists**: late readings the speed layer dropped are recovered, and the drift % is measured. |
| 9 | 7:30-8:30 | Terminal: `issue_tariff_correction.py` + trigger `smartgrid_recompute` | Recompute from the immutable archive: the corrected tariff (v2) regenerates that day's bills and report. Show `tariff_version` = 2 in the batch runs table. |
| 10 | 8:30-9:30 | `docker compose stop meter-simulator` → Prometheus alerts → Grafana ops-alert table | Observability: `MeterStreamNoData`, `SpeedLayerNoInput` and `SpeedViewStale` fire, arrive via Alertmanager webhook, get logged. Restart, and the alerts resolve. |
| 11 | 9:30-10:00 | README limitations | Honest limitations and what would change at production scale. |

## Commands used in the demo

```bash
docker compose ps
docker compose logs -f --tail 20 meter-simulator
curl -s localhost:8000/api/v1/realtime/zones | python -m json.tool
curl -s localhost:8000/api/v1/households/HH-00001/bill-estimate | python -m json.tool
curl -s localhost:8000/api/v1/billing/days
curl -s localhost:8000/api/v1/pipeline/status | python -m json.tool | head -40

# recompute after a tariff correction (use a billed day from /api/v1/billing/days)
docker compose exec airflow-scheduler python /opt/smartgrid/scripts/issue_tariff_correction.py 2026-03-02 --pct 10
docker compose exec airflow-scheduler airflow dags trigger smartgrid_recompute \
  -c '{"start_date": "2026-03-02", "end_date": "2026-03-02", "reason": "tariff correction v2"}'

# failure drill
docker compose stop meter-simulator      # wait ~90 s, then check http://localhost:9090/alerts
docker compose start meter-simulator
```

## Screenshots to capture for the report

1. Grafana *Live Grid Operations* (full page), ideally on a cloudy day with LOW_RENEWABLE alerts.
2. Grafana *Daily Billing & Solar Contribution* for one day, including the speed-vs-batch panel.
3. Grafana *Pipeline Health* (throughput, lag, latency, invalid reasons, batch stats).
4. Airflow DAG graph with a successful run, plus the Gantt view (task durations).
5. Airflow `smartgrid_recompute` run after a tariff correction.
6. Spark UI → Structured Streaming → `grid_state` (input rate, processing rate, batch duration).
7. The daily HTML report (top section + reconciliation + data-quality section).
8. `/docs` Swagger page and a JSON response of `/bill-estimate`.
9. Prometheus `/alerts` with a firing alert, and the ops-alerts table after the failure drill.
10. A few lines of structured JSON logs from 2-3 services.
