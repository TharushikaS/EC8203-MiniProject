"""Python callables used by the Airflow DAGs (kept out of the DAG files so they are testable)."""
from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timedelta, timezone

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from smartgrid.batch.reference_loader import find_tariff_file, load_reference_for_day
from smartgrid.common import db
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.logging_utils import get_logger
from smartgrid.common.sim_clock import get_clock

log = get_logger("batch-layer", "orchestration", name="batch.tasks")


# --------------------------------------------------------------------------- scheduling
def resolve_target_day(settings: Settings | None = None) -> str | None:
    """Pick the oldest *completed* simulated day (within the look-back window) that has raw
    data and no successful daily run yet. Returns ISO date or None (-> downstream skipped).

    A day is only eligible once simulated time is past midnight + BATCH_GRACE_SIM_MINUTES,
    giving late meter readings a chance to land in the archive before we bill.
    """
    settings = settings or get_settings()
    clock = get_clock(settings)
    now = clock.now()
    with db.transaction(settings) as cur:
        cur.execute("SELECT bill_date FROM batch_runs WHERE status = 'SUCCESS' AND run_type = 'daily'")
        done = {r[0] for r in cur.fetchall()}
    for k in range(settings.batch_lookback_days, 0, -1):
        day = now.date() - timedelta(days=k)
        if day < clock.sim_start.date() or day in done:
            continue
        cutoff = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc) + \
            timedelta(minutes=settings.batch_grace_sim_minutes)
        if now < cutoff:
            continue
        if not os.path.isdir(os.path.join(settings.raw_readings_path, f"event_date={day.isoformat()}")):
            continue
        log.info("batch_target_resolved", sim_date=day.isoformat(), sim_now=now.isoformat())
        return day.isoformat()
    log.info("batch_nothing_to_do", sim_now=now.isoformat(), lookback_days=settings.batch_lookback_days)
    return None


def tariff_file_ready(day: str, settings: Settings | None = None) -> bool:
    settings = settings or get_settings()
    found = find_tariff_file(date.fromisoformat(day), settings)
    if not found:
        log.info("waiting_for_tariff_feed", sim_date=day)
    return bool(found)


def load_reference(day: str) -> dict:
    return load_reference_for_day(date.fromisoformat(day))


def generate_report(day: str) -> str:
    # Imported lazily so DAG parsing does not pay for matplotlib/jinja imports.
    from smartgrid.batch.report import generate_daily_report
    return generate_daily_report(day)


def days_in_range(start: str, end: str) -> list[str]:
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    if e < s:
        raise ValueError("end_date must be >= start_date")
    return [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]


# --------------------------------------------------------------------------- reconciliation
RECONCILE_SQL = """
INSERT INTO speed_batch_reconciliation
    (bill_date, grid_zone, speed_consumption_kwh, batch_consumption_kwh, speed_solar_kwh, batch_solar_kwh,
     drift_kwh, drift_pct, late_records, computed_at)
SELECT b.bill_date, b.grid_zone,
       COALESCE(s.cons, 0), b.consumption_kwh, COALESCE(s.sol, 0), b.solar_kwh,
       round((b.consumption_kwh - COALESCE(s.cons, 0))::numeric, 4),
       CASE WHEN b.consumption_kwh > 0
            THEN round(((b.consumption_kwh - COALESCE(s.cons, 0)) / b.consumption_kwh * 100)::numeric, 3) END,
       COALESCE(l.late, 0), now()
FROM batch_zone_daily_summary b
LEFT JOIN (SELECT grid_zone, sum(consumption_kwh) AS cons, sum(solar_kwh) AS sol
           FROM rt_zone_metrics
           WHERE window_start >= %(d)s::date AND window_start < %(d)s::date + 1
           GROUP BY grid_zone) s ON s.grid_zone = b.grid_zone
LEFT JOIN (SELECT grid_zone, sum(late_readings) AS late
           FROM batch_household_daily_bill WHERE bill_date = %(d)s::date GROUP BY grid_zone) l
       ON l.grid_zone = b.grid_zone
WHERE b.bill_date = %(d)s::date
ON CONFLICT (bill_date, grid_zone) DO UPDATE SET
    speed_consumption_kwh = EXCLUDED.speed_consumption_kwh, batch_consumption_kwh = EXCLUDED.batch_consumption_kwh,
    speed_solar_kwh = EXCLUDED.speed_solar_kwh, batch_solar_kwh = EXCLUDED.batch_solar_kwh,
    drift_kwh = EXCLUDED.drift_kwh, drift_pct = EXCLUDED.drift_pct, late_records = EXCLUDED.late_records,
    computed_at = EXCLUDED.computed_at
"""


def reconcile_speed_vs_batch(day: str, settings: Settings | None = None) -> dict:
    """Compare the speed layer's approximate zone totals with the batch truth for ``day``.
    The drift quantifies exactly what the speed layer missed (late / dropped readings)."""
    settings = settings or get_settings()
    with db.transaction(settings) as cur:
        cur.execute(RECONCILE_SQL, {"d": day})
        cur.execute("""SELECT sum(speed_consumption_kwh), sum(batch_consumption_kwh), sum(late_records)
                       FROM speed_batch_reconciliation WHERE bill_date = %s""", (day,))
        speed, batch, late = cur.fetchone()
    speed, batch = float(speed or 0), float(batch or 0)
    result = {"sim_date": day, "speed_kwh": round(speed, 3), "batch_kwh": round(batch, 3),
              "drift_kwh": round(batch - speed, 3),
              "drift_pct": round((batch - speed) / batch * 100, 3) if batch else None,
              "late_records": int(late or 0)}
    log.info("speed_batch_reconciled", **result)
    return result


# --------------------------------------------------------------------------- observability
def publish_batch_metrics(day: str, dag_id: str, status: int, settings: Settings | None = None) -> None:
    """Push batch-run metrics to the Prometheus Pushgateway (batch jobs are too short-lived
    to be scraped). ``status`` 1 = success, 0 = failure."""
    settings = settings or get_settings()
    reg = CollectorRegistry()
    g = lambda n, d: Gauge(n, d, registry=reg)  # noqa: E731
    last_status = g("smartgrid_batch_last_status", "1 if the last batch run succeeded, else 0")
    last_status.set(status)
    g("smartgrid_batch_last_run_unixtime", "Real time of the last batch run").set(time.time())
    sim_day = datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()
    if status:
        g("smartgrid_batch_last_success_unixtime", "Real time of the last successful run").set(time.time())
        g("smartgrid_batch_last_success_sim_day", "Simulated day (epoch s) of the last success").set(sim_day)
        try:
            with db.transaction(settings) as cur:
                cur.execute("""SELECT raw_records, invalid_records, duplicates_removed, late_records,
                                      households_billed, total_billed, duration_seconds
                               FROM batch_runs WHERE bill_date = %s AND status = 'SUCCESS'
                               ORDER BY finished_at DESC LIMIT 1""", (day,))
                row = cur.fetchone()
                cur.execute("""SELECT COALESCE(max(abs(drift_pct)), 0) FROM speed_batch_reconciliation
                               WHERE bill_date = %s""", (day,))
                drift = cur.fetchone()[0]
            if row:
                names = ["raw_records", "invalid_records", "duplicates_removed", "late_records",
                         "households_billed", "total_billed", "duration_seconds"]
                for name, value in zip(names, row):
                    g(f"smartgrid_batch_last_{name}", f"Last successful batch run: {name}").set(float(value or 0))
            g("smartgrid_batch_last_max_zone_drift_pct", "Max |speed-batch| drift % across zones").set(float(drift))
        except Exception:  # metrics must never fail the pipeline
            log.exception("batch_metrics_query_failed", sim_date=day)
    try:
        push_to_gateway(settings.pushgateway_url, job="smartgrid_batch", grouping_key={"dag": dag_id}, registry=reg)
        log.info("batch_metrics_pushed", sim_date=day, dag_id=dag_id, status=status)
    except Exception as exc:
        log.warning("pushgateway_unreachable", error=str(exc))


def mark_run_failed(run_id: str, day: str | None, run_type: str, error: str,
                    settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    if not day:
        return
    try:
        with db.transaction(settings) as cur:
            cur.execute(
                """INSERT INTO batch_runs (run_id, bill_date, run_type, status, error, started_at, finished_at)
                   VALUES (%s, %s, %s, 'FAILED', %s, now(), now())
                   ON CONFLICT (run_id, bill_date) DO UPDATE SET status = 'FAILED', error = EXCLUDED.error,
                       finished_at = now()""",
                (run_id, day, run_type, error[:4000]),
            )
    except Exception:
        log.exception("could_not_record_failure", run_id=run_id)


def airflow_failure_callback(context) -> None:
    """on_failure_callback: structured log + failed batch_runs row + failure metric."""
    ti = context["task_instance"]
    dag_run = context["dag_run"]
    day = ti.xcom_pull(task_ids="resolve_target_day") if dag_run.dag_id.endswith("daily_batch") else None
    err = str(context.get("exception"))
    log.error("airflow_task_failed", dag_id=dag_run.dag_id, task_id=ti.task_id, run_id=dag_run.run_id,
              sim_date=day, try_number=ti.try_number, error=err)
    mark_run_failed(dag_run.run_id, day, "daily", f"{ti.task_id}: {err}")
    publish_batch_metrics(day or date.today().isoformat(), dag_run.dag_id, status=0)


def dump(obj) -> str:
    return json.dumps(obj, default=str)
