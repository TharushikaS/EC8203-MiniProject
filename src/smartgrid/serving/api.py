"""SERVING LAYER - FastAPI application.

Endpoints (all JSON, OpenAPI docs at /docs):
  GET  /health                                   liveness + freshness of speed and batch views
  GET  /api/v1/realtime/grid                     grid-wide load & renewable mix right now
  GET  /api/v1/realtime/zones                    per-zone load & renewable mix right now (speed view)
  GET  /api/v1/realtime/zones/{zone}/history     recent windows for one zone
  GET  /api/v1/alerts                            threshold alerts raised by the speed layer
  GET  /api/v1/households/{id}/bills             confirmed daily bills (batch view)
  GET  /api/v1/households/{id}/bill-estimate     month-to-date: batch + speed merged (Lambda merge)
  GET  /api/v1/billing/{date}/zones              daily zone summary (batch view)
  GET  /api/v1/billing/{date}/households         daily bills for all households (paged)
  GET  /api/v1/billing/{date}/reconciliation     speed-vs-batch drift for a day
  GET  /api/v1/reports                           list generated daily reports
  GET  /api/v1/reports/{date}                    the consolidated HTML report
  GET  /api/v1/pipeline/status                   batch runs, stream quality, latency, freshness
  GET  /api/v1/pipeline/traces[/{event_id}]      sampled end-to-end event traces
  POST /api/v1/ops/alertmanager                  Alertmanager webhook receiver (ops alert log)
  GET  /metrics                                  Prometheus metrics (incl. business gauges)
"""
from __future__ import annotations

import json
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from prometheus_client import Counter, Gauge, Histogram, make_asgi_app

from smartgrid.common.config import get_settings
from smartgrid.common.households import GRID_ZONES
from smartgrid.common.logging_utils import get_logger
from smartgrid.common.sim_clock import get_clock
from smartgrid.serving.lambda_merge import merge_month_to_date
from smartgrid.serving.repository import Repository

log = get_logger("serving-api", "serving")
settings = get_settings()

REQUESTS = Counter("smartgrid_api_requests_total", "API requests", ["path", "status"])
LATENCY = Histogram("smartgrid_api_request_seconds", "API latency", ["path"])
ZONE_LOAD = Gauge("smartgrid_zone_load_kw", "Current zone load (speed view)", ["zone"])
ZONE_SOLAR = Gauge("smartgrid_zone_solar_kw", "Current zone solar generation (speed view)", ["zone"])
ZONE_SHARE = Gauge("smartgrid_zone_renewable_share", "Current zone renewable share (speed view)", ["zone"])
SPEED_AGE = Gauge("smartgrid_speed_view_age_seconds", "Seconds since the speed view was last updated")
BATCH_LAG = Gauge("smartgrid_batch_view_lag_days", "Simulated days between today and the latest billed day")
SIM_HOUR = Gauge("smartgrid_sim_hour", "Current simulated hour of day")
SIM_NOW = Gauge("smartgrid_sim_time_seconds", "Current simulated epoch seconds")
RECENT_ALERTS = Gauge("smartgrid_grid_alerts_recent", "Grid alerts raised in the last 5 real minutes")


def _refresh_business_gauges(repo: Repository, stop: threading.Event) -> None:
    """Background thread: expose serving-store state as Prometheus gauges every 10 s so
    Prometheus alert rules can watch freshness and business thresholds."""
    clock = get_clock(settings, wait=True)
    while not stop.is_set():
        try:
            now = clock.now()
            SIM_HOUR.set(now.hour + now.minute / 60)
            SIM_NOW.set(now.timestamp())
            for z in repo.latest_zone_metrics():
                ZONE_LOAD.labels(zone=z["grid_zone"]).set(z["load_kw"])
                ZONE_SOLAR.labels(zone=z["grid_zone"]).set(z["solar_kw"])
                ZONE_SHARE.labels(zone=z["grid_zone"]).set(z["renewable_share"])
            f = repo.freshness()
            if f.get("speed_age_s") is not None:
                SPEED_AGE.set(float(f["speed_age_s"]))
            if f.get("batch_latest_day"):
                BATCH_LAG.set((now.date() - f["batch_latest_day"]).days)
            RECENT_ALERTS.set(int(f.get("recent_alerts") or 0))
        except Exception as exc:     # never crash the API because of metrics
            log.warning("gauge_refresh_failed", error=str(exc))
        stop.wait(10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for _ in range(60):
        try:
            app.state.repo = Repository(settings)
            break
        except Exception as exc:
            log.warning("waiting_for_postgres", error=str(exc))
            time.sleep(2)
    else:
        raise RuntimeError("PostgreSQL not reachable")
    app.state.clock = get_clock(settings, wait=True)
    stop = threading.Event()
    threading.Thread(target=_refresh_business_gauges, args=(app.state.repo, stop), daemon=True).start()
    log.info("api_started", sim_clock=app.state.clock.as_dict())
    yield
    stop.set()


app = FastAPI(title="SmartGrid Serving API", version="1.0.0", lifespan=lifespan,
              description="Lambda serving layer: real-time grid views (speed layer) and daily bills (batch layer).")
app.mount("/metrics", make_asgi_app())


@app.middleware("http")
async def observe(request: Request, call_next):
    route_path = request.url.path
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        REQUESTS.labels(path=route_path, status="500").inc()
        log.exception("request_failed", path=route_path)
        raise
    route = request.scope.get("route")
    template = getattr(route, "path", route_path)
    LATENCY.labels(path=template).observe(time.perf_counter() - start)
    REQUESTS.labels(path=template, status=str(response.status_code)).inc()
    return response


def repo(request: Request) -> Repository:
    return request.app.state.repo


def _sim_now(request: Request):
    return request.app.state.clock.now()


# --------------------------------------------------------------------------- health
@app.get("/health")
def health(request: Request):
    checks = {}
    try:
        f = repo(request).freshness()
        checks["database"] = "ok"
    except Exception as exc:
        return JSONResponse(status_code=503, content={"status": "down", "database": str(exc)})
    sim_now = _sim_now(request)
    speed_age = float(f["speed_age_s"]) if f.get("speed_age_s") is not None else None
    checks["speed_view_age_seconds"] = speed_age
    checks["speed_view"] = "ok" if speed_age is not None and speed_age < 60 else "stale"
    lag = (sim_now.date() - f["batch_latest_day"]).days if f.get("batch_latest_day") else None
    checks["batch_latest_day"] = f["batch_latest_day"].isoformat() if f.get("batch_latest_day") else None
    checks["batch_view_lag_sim_days"] = lag
    checks["batch_view"] = "ok" if lag is not None and lag <= 2 else "stale"
    status = "ok" if checks["speed_view"] == "ok" and checks["batch_view"] == "ok" else "degraded"
    return {"status": status, "sim_time": sim_now.isoformat(), "checks": checks}


# --------------------------------------------------------------------------- real-time (speed)
@app.get("/api/v1/realtime/zones")
def realtime_zones(request: Request):
    rows = repo(request).latest_zone_metrics()
    return {"sim_time": _sim_now(request).isoformat(), "source": "speed", "zones": [
        {"grid_zone": r["grid_zone"], "window_start": r["window_start"].isoformat(),
         "window_end": r["window_end"].isoformat(), "load_kw": round(r["load_kw"], 2),
         "solar_kw": round(r["solar_kw"], 2), "renewable_share": round(r["renewable_share"], 4),
         "active_meters": r["active_meters"], "expected_meters": r["expected_meters"],
         "meter_coverage": round(r["active_meters"] / r["expected_meters"], 3) if r["expected_meters"] else None,
         "low_renewable": r["renewable_share"] < settings.low_renewable_threshold and 9 <= r["window_start"].hour < 16,
         "updated_at": r["updated_at"].isoformat()} for r in rows]}


@app.get("/api/v1/realtime/grid")
def realtime_grid(request: Request):
    rows = repo(request).latest_zone_metrics()
    load = sum(r["load_kw"] for r in rows)
    solar = sum(r["solar_kw"] for r in rows)
    return {"sim_time": _sim_now(request).isoformat(), "source": "speed", "zones_reporting": len(rows),
            "total_load_kw": round(load, 2), "total_solar_kw": round(solar, 2),
            "renewable_share": round(min(solar, load) / load, 4) if load else 0.0,
            "active_meters": sum(r["active_meters"] for r in rows),
            "lowest_renewable_zone": min(rows, key=lambda r: r["renewable_share"])["grid_zone"] if rows else None}


@app.get("/api/v1/realtime/zones/{zone}/history")
def zone_history(request: Request, zone: str, limit: int = Query(24, ge=1, le=500)):
    zone = zone.upper()
    if zone not in GRID_ZONES:
        raise HTTPException(404, f"Unknown zone {zone}; valid: {', '.join(GRID_ZONES)}")
    return {"grid_zone": zone, "windows": repo(request).zone_history(zone, limit)}


@app.get("/api/v1/alerts")
def alerts(request: Request, limit: int = Query(50, ge=1, le=500), alert_type: str | None = None,
           zone: str | None = None):
    return {"alerts": repo(request).alerts(limit, alert_type, zone.upper() if zone else None)}


# --------------------------------------------------------------------------- billing (batch + merge)
@app.get("/api/v1/households/{household_id}/bills")
def household_bills(request: Request, household_id: str, limit: int = Query(31, ge=1, le=366)):
    rows = repo(request).household_bills(household_id, limit=limit)
    if not rows:
        raise HTTPException(404, f"No confirmed bills for {household_id}")
    return {"household_id": household_id, "source": "batch", "bills": rows}


@app.get("/api/v1/households/{household_id}/bill-estimate")
def bill_estimate(request: Request, household_id: str):
    r = repo(request)
    today = _sim_now(request).date()
    month_start = today.replace(day=1)
    batch_rows = r.household_bills(household_id, since=month_start)
    speed_rows = r.household_speed_days(household_id, month_start)
    if not batch_rows and not speed_rows:
        raise HTTPException(404, f"No data for {household_id}")
    merged = merge_month_to_date(batch_rows, speed_rows, r.latest_tariff(household_id), settings.readings_per_day)
    return {"household_id": household_id, "period_start": month_start.isoformat(), "sim_date": today.isoformat(),
            "currency": settings.currency, **merged}


@app.get("/api/v1/billing/{bill_date}/zones")
def billing_zones(request: Request, bill_date: date):
    rows = repo(request).zone_daily(bill_date)
    if not rows:
        raise HTTPException(404, f"No batch view for {bill_date} yet")
    return {"bill_date": bill_date.isoformat(), "source": "batch", "currency": settings.currency, "zones": rows}


@app.get("/api/v1/billing/{bill_date}/households")
def billing_households(request: Request, bill_date: date, zone: str | None = None,
                       limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    rows = repo(request).bills_for_day(bill_date, zone.upper() if zone else None, limit, offset)
    return {"bill_date": bill_date.isoformat(), "source": "batch", "count": len(rows), "bills": rows}


@app.get("/api/v1/billing/{bill_date}/reconciliation")
def reconciliation(request: Request, bill_date: date):
    return {"bill_date": bill_date.isoformat(), "zones": repo(request).reconciliation(bill_date)}


@app.get("/api/v1/billing/days")
def billed_days(request: Request):
    return {"days": [d.isoformat() for d in repo(request).batch_days()]}


# --------------------------------------------------------------------------- reports
def _reports_dir() -> str:
    return os.path.join(settings.output_dir, "reports")


@app.get("/api/v1/reports")
def list_reports():
    base = _reports_dir()
    days = sorted(os.listdir(base), reverse=True) if os.path.isdir(base) else []
    return {"reports": [{"date": d, "url": f"/api/v1/reports/{d}"} for d in days]}


@app.get("/api/v1/reports/{bill_date}")
def get_report(bill_date: date):
    path = os.path.join(_reports_dir(), bill_date.isoformat(), f"daily_report_{bill_date.isoformat()}.html")
    if not os.path.exists(path):
        raise HTTPException(404, f"Report for {bill_date} not generated yet")
    return FileResponse(path, media_type="text/html")


# --------------------------------------------------------------------------- pipeline observability
@app.get("/api/v1/pipeline/status")
def pipeline_status(request: Request, limit: int = Query(10, ge=1, le=100)):
    r = repo(request)
    return {"sim_time": _sim_now(request).isoformat(), "freshness": r.freshness(),
            "stream_quality_last_10min": r.quality_summary(), "e2e_latency_last_10min": r.latency_summary(),
            "batch_runs": r.batch_runs(limit), "ops_alerts": r.ops_alerts(10)}


@app.get("/api/v1/pipeline/traces")
def traces(request: Request, limit: int = Query(20, ge=1, le=200)):
    return {"traces": repo(request).recent_traces(limit)}


@app.get("/api/v1/pipeline/traces/{event_id}")
def trace(request: Request, event_id: str):
    t = repo(request).trace(event_id)
    if not t:
        raise HTTPException(404, "Event not sampled for tracing (1% sample) or unknown")
    stages = [("produced", t["produced_at"]), ("kafka_appended", t["kafka_ts"]),
              ("speed_processed", t["speed_processed_at"])]
    return {**t, "stages": [{"stage": s, "at": ts.isoformat() if ts else None} for s, ts in stages]}


@app.post("/api/v1/ops/alertmanager")
async def alertmanager_webhook(request: Request):
    payload = await request.json()
    for a in payload.get("alerts", []):
        labels = a.get("labels", {})
        ann = a.get("annotations", {})
        repo(request).insert_ops_alert((labels.get("alertname", "unknown"), a.get("status", "firing"),
                                        labels.get("severity"), ann.get("summary"), json.dumps(labels),
                                        a.get("startsAt"), None if a.get("status") == "firing" else a.get("endsAt")))
        log.warning("ops_alert_received", alertname=labels.get("alertname"), status=a.get("status"),
                    severity=labels.get("severity"), summary=ann.get("summary"))
    return {"received": len(payload.get("alerts", []))}
