# Demo guide (8-10 minute video or live demo)

The brief asks for a 5-10 minute video *showing the pipeline running end-to-end and the
observability results*. Everything is shown **in a browser plus one terminal** on the machine
running `docker compose`. Nothing is deployed anywhere else.

## 1. Where and how to record

* **Machine:** the laptop that runs the stack (all UIs are on `localhost`).
* **Recorder:** OBS Studio (free), or Windows **Win + Alt + R** (Xbox Game Bar, records one window),
  or the Win 11 Snipping Tool video mode. Record at 1920×1080. Use a headset microphone.
* **Group video:** each member records their own part (same machine or screen-share on a Teams/Zoom
  call with "record"), then the parts are joined in Clipchamp (built into Windows) or any editor.
* **Browser:** one Chrome window with these tabs **in this order**, all logged in beforehand:

| Tab | URL | Login |
|---|---|---|
| 1 README (architecture diagram) | the GitHub repo page | - |
| 2 Grafana Live Ops | http://localhost:3000/d/smartgrid-live-ops | anonymous view |
| 3 Spark UI | http://localhost:4040/StreamingQuery/ | - |
| 4 API docs | http://localhost:8000/docs | - |
| 5 Airflow | http://localhost:8080/dags/smartgrid_daily_batch/grid | admin / admin |
| 6 Daily report | http://localhost:8000/api/v1/reports/<a billed day> | - |
| 7 Grafana Billing | http://localhost:3000/d/smartgrid-billing | - |
| 8 Grafana Pipeline Health | http://localhost:3000/d/smartgrid-pipeline | - |
| 9 Prometheus alerts | http://localhost:9090/alerts | - |

* **Terminal:** PowerShell in the project folder, font size about 16, window beside the browser.

## 2. Preparation checklist (do this 20-30 minutes before recording)

1. **Turn off sleep**: Settings → System → Power → Screen and sleep → *Never* (while plugged in).
   If the laptop sleeps, the simulated clock jumps ahead and leaves a gap.
2. Close heavy apps (browsers with many tabs, IDE indexing). The laptop has only 4 cores.
3. Fresh start so the timeline is clean:
   ```powershell
   docker compose down -v
   docker compose up -d
   ```
4. Wait **15-20 minutes**, so 2-3 simulated days are billed. Check:
   ```powershell
   curl.exe -s localhost:8000/health
   curl.exe -s localhost:8000/api/v1/billing/days
   python scripts/smoke_test.py
   ```
   `health` should say `"status":"ok"` and `days` should list at least 2 dates. Use the **oldest
   billed day** (e.g. `2026-03-01`) wherever `<DAY>` appears below.
5. Open all tabs from the table and refresh each one once.
6. Do one dry run of the whole script.

## 3. Script (about 9 minutes, three presenters)

The **Show** column is what is on screen; the **Say** column is the narration (paraphrase freely).

### Part A - Presenter 1: problem, architecture, ingestion (0:00-3:00)

| Time | Show | Say |
|---|---|---|
| 0:00 | Tab 1: README, architecture diagram | "We built a data platform for **Use Case 3, Smart Grid Energy Monitoring and Billing**. The business question is: what is the current grid load and solar contribution by zone, and what will each household's bill be once the daily tariff is applied." |
| 0:30 | Same diagram, point at the two paths | "We chose a **Lambda architecture**. Bills are money: they must be exact, auditable and correctable, and the tariff arrives only once a day, so billing is a **batch layer** that recomputes from an immutable raw archive. Operators also need live grid visibility, so a **speed layer** gives second-level but approximate views. We rejected Kappa because correcting bills would mean replaying months of Kafka history, and late meter data would keep changing 'final' bills." |
| 1:15 | Terminal: `docker compose ps` | "Everything runs with one Docker Compose command: Kafka, Spark, Airflow, Postgres, the API, Prometheus and Grafana. Simulated time is compressed: **one day is five real minutes**." |
| 1:35 | Terminal: `docker compose logs --tail 5 meter-simulator` | "The streaming source is a Python simulator for 250 smart meters. Every 15 simulated minutes each meter sends a reading to Kafka, about 80 events per second. Logs are **structured JSON**. We inject realistic faults on purpose: duplicate sends, invalid readings, and meters that go offline and upload hours later." |
| 2:15 | Tab 8 Pipeline Health, "Kafka messages/s per partition" and "Injected faults" panels | "The topic has 6 partitions, keyed by household, so each meter's readings stay in order while the load spreads evenly, as you can see here." |
| 2:35 | Terminal: `docker compose exec tariff-simulator ls /data/landing/tariffs` | "The batch source is the billing system's daily extract: one tariff file per day plus a weather file. Rates change daily, some rows are deliberately bad, and some days the file lands late." |

### Part B - Presenter 2: speed layer, serving, batch layer (3:00-6:30)

| Time | Show | Say |
|---|---|---|
| 3:00 | Tab 3 Spark UI, click `grid_state` | "The speed layer is Spark Structured Streaming. `raw_ingest` writes every record to the Parquet master dataset and validates it; rejects go to a dead-letter topic. `grid_state` uses a **2-hour event-time watermark**, drops duplicates within it, and keeps 1-hour windows per household. Micro-batches take a few seconds." |
| 3:40 | Tab 2 Grafana Live Ops (scroll slowly) | "This is the live operations view: load, solar and renewable share per zone. When a zone's renewable share drops below 25% in daylight, for example on a cloudy day, the speed layer raises a **LOW_RENEWABLE** alert, which also goes to a Kafka alerts topic." |
| 4:20 | Tab 4 API docs → `GET /api/v1/realtime/zones` → *Try it out* → *Execute* | "The serving API gives the same real-time figures to other systems." |
| 4:40 | `GET /api/v1/households/{id}/bill-estimate` with `HH-00001` | "This is the **Lambda merge**. Completed days come from the batch layer and are marked `final`. Today comes from the speed layer, priced with the same billing function, and is marked `provisional`." |
| 5:10 | Tab 5 Airflow grid view, click one green run, then *Graph* | "The batch layer is orchestrated by Airflow. For each finished day a sensor waits for the tariff file, the reference data is validated and bad rows quarantined, then a **Spark batch job** recomputes the day from the raw archive: exact dedup, join with the tariff, pricing, zone and hourly summaries. Then reconciliation, the report and metrics. Each day takes about a minute." |
| 5:50 | Tab 6 daily report (scroll: summary → zones → bills → reconciliation → lineage) | "This is the consolidated daily report that answers the business question: renewable contribution by zone and by hour, every household's bill with its breakdown, and data-quality lineage: records read, duplicates removed, invalid rejected, late readings recovered." |

### Part C - Presenter 3: why Lambda pays off, observability, limitations (6:30-9:30)

| Time | Show | Say |
|---|---|---|
| 6:30 | Tab 7 Grafana Billing, "Speed vs batch" panel and reconciliation table | "Here you can see why we need the batch layer. The speed layer dropped readings that arrived after its watermark. The batch layer recovered them, and this table measures the difference per zone." |
| 7:00 | Terminal: correction command (below, with `--recompute`) | "Billing systems issue corrections. Here the billing system re-issues this day's tariff with rates 10% higher..." |
| 7:15 | Tab 5 Airflow → `smartgrid_recompute` running (it takes about 1.5 min) | "...and we recompute that day from the immutable archive. No data is replayed from Kafka." |
| 7:45 | Tab 7 Billing → *Batch runs* table (refresh): new row `recompute`, tariff v2 | "The day was rebuilt with tariff version 2, and the lineage keeps both runs, so it's fully auditable." |
| 8:05 | Terminal: `docker compose stop meter-simulator`; switch to Tab 8, then Tab 9 | "Observability: every stage exports Prometheus metrics, such as throughput, Kafka lag, micro-batch time, end-to-end latency and invalid records by reason, and there are 14 alert rules. Let's break the pipeline by stopping the meter feed." |
| 8:40 | Tab 9 Prometheus alerts: `MeterStreamNoData`, `SpeedLayerNoInput`, `SpeedViewStale` firing; then Tab 2 bottom "Operational alerts" table | "Within about two minutes the no-data and stale-view alerts fire. Alertmanager delivers them to our API, which logs them and shows them here." |
| 9:05 | Terminal: `docker compose start meter-simulator` | "After restarting the feed, the alerts resolve by themselves." |
| 9:15 | README "Assumptions" section | "Limitations: everything is single-node, with one Kafka broker and Spark in local mode. At production scale we would use a replicated Kafka cluster, Spark on Kubernetes, and a table format like Delta Lake on S3 for the master dataset. Thank you." |

> **Timing tip for the failure drill:** the alerts take about 2 minutes to fire. Either stop the
> meter simulator at 8:05 and cut the waiting time out in editing, or stop it before starting
> Part C and show the already-firing alerts.

## 4. Commands (copy-paste, PowerShell)

```powershell
docker compose ps
docker compose logs --tail 5 meter-simulator
docker compose exec tariff-simulator ls /data/landing/tariffs

# tariff correction + recompute in one command (replace 2026-03-01 with your oldest billed day)
docker compose exec airflow-scheduler python /opt/smartgrid/scripts/issue_tariff_correction.py 2026-03-01 --pct 10 --recompute

# failure drill
docker compose stop meter-simulator
docker compose start meter-simulator
```

The same commands work unchanged in PowerShell 5/7, cmd and Git Bash. (In the PDF version, long commands are wrapped with a trailing backtick character, which is PowerShell's line continuation; in cmd or bash, type them on one line.)

## 5. Screenshots to capture for the report

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
