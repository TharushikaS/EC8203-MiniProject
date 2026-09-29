"""DAILY-BATCH SOURCE - billing-system extract + weather file, dropped once per simulated day.

Shortly after simulated midnight (00:15 by default) the "billing system" publishes the
tariff that applied to the day that just ended, and the "met office" publishes observed
weather for that day plus a forecast for the next day:

    /data/landing/tariffs/tariffs_<YYYY-MM-DD>.csv
        household_id, tariff_plan, tariff_rate, billing_tier, subsidy_flag,
        feed_in_rate, effective_date, published_at
    /data/landing/weather/weather_<YYYY-MM-DD>.json
        {date, zones: {ZONE: {cloud_cover, solar_potential_index, forecast_next_day_cloud_cover}}}

Tariffs change every day (a fuel-cost adjustment factor), so billing genuinely needs the
daily file - consumption cannot be priced until it lands. That is the business reason
the billing view is computed in the batch layer.

Files are written to a temp name and atomically renamed, so the Airflow sensor never sees
a half-written file. Faults injected on purpose: bad rows (negative rate, unknown tier,
duplicate household, blank fields) and late delivery (up to a few simulated hours).
"""
from __future__ import annotations

import csv
import glob
import json
import os
import random
import signal
import time
from datetime import date, datetime, timedelta, timezone

from prometheus_client import Counter, Gauge, start_http_server

from smartgrid.common.config import Settings, get_settings
from smartgrid.common.energy_model import _unit_hash, clear_sky_factor, daily_cloud_cover
from smartgrid.common.households import GRID_ZONES, build_registry
from smartgrid.common.logging_utils import get_logger
from smartgrid.common.sim_clock import SimClock, get_clock

log = get_logger("tariff-simulator", "ingestion")

FILES_DROPPED = Counter("smartgrid_batch_source_files_dropped_total", "Daily files published", ["feed"])
ROWS_WRITTEN = Counter("smartgrid_batch_source_rows_written_total", "Tariff rows written")
BAD_ROWS = Counter("smartgrid_batch_source_bad_rows_injected_total", "Deliberately bad tariff rows", ["fault"])
LAST_DROP = Gauge("smartgrid_tariff_last_drop_unixtime", "Real time the last tariff file was published")
LAST_DROP_DAY = Gauge("smartgrid_tariff_last_drop_sim_day", "Simulated day (epoch s) covered by the last file")

BASE_RATE = {"FLAT": 32.0, "TOU": 30.0, "GREEN": 34.0}          # LKR per kWh
FEED_IN_RATE = {"FLAT": 22.0, "TOU": 22.0, "GREEN": 27.0}       # LKR per exported kWh
DROP_OFFSET_SIM_MINUTES = 15


def fuel_adjustment(day: date, seed: int) -> float:
    """Daily fuel-cost adjustment factor in [0.92, 1.12] - makes tariffs vary by day."""
    return round(0.92 + 0.20 * _unit_hash("fuel", day.isoformat(), seed), 4)


def delivery_delay_minutes(day: date, settings: Settings) -> int:
    """Deterministic per-day delivery delay; some days the feed arrives hours late."""
    if _unit_hash("late", day.isoformat(), settings.random_seed) < settings.tariff_late_probability:
        return 60 + int(_unit_hash("late-mins", day.isoformat(), settings.random_seed) * 150)
    return 0


def build_tariff_rows(day: date, settings: Settings, rng: random.Random) -> list[dict]:
    factor = fuel_adjustment(day, settings.random_seed)
    published = datetime.now(timezone.utc).isoformat()
    rows = []
    for h in build_registry(settings.num_households, settings.random_seed):
        rows.append({
            "household_id": h.household_id,
            "tariff_plan": h.tariff_plan,
            "tariff_rate": round(BASE_RATE[h.tariff_plan] * factor, 3),
            "billing_tier": h.billing_tier,
            "subsidy_flag": str(h.subsidy_flag).lower(),
            "feed_in_rate": FEED_IN_RATE[h.tariff_plan],
            "effective_date": day.isoformat(),
            "published_at": published,
        })
    # --- inject realistic data-quality problems -----------------------------------
    n_bad = sum(1 for _ in rows if rng.random() < settings.tariff_bad_row_rate)
    for _ in range(n_bad):
        fault = rng.choice(["negative_rate", "unknown_tier", "duplicate_row", "blank_rate"])
        victim = dict(rng.choice(rows))
        if fault == "negative_rate":
            victim["tariff_rate"] = -victim["tariff_rate"]
        elif fault == "unknown_tier":
            victim["billing_tier"] = "PLATINUM"
        elif fault == "blank_rate":
            victim["tariff_rate"] = ""
        # duplicate_row: append unchanged copy -> validator must de-duplicate
        rows.append(victim)
        BAD_ROWS.labels(fault=fault).inc()
    rng.shuffle(rows)
    return rows


def build_weather(day: date, settings: Settings) -> dict:
    zones = {}
    for zone in GRID_ZONES:
        cc = daily_cloud_cover(day, zone, settings.random_seed)
        potential = sum(clear_sky_factor(h + 0.5) for h in range(24)) * (1 - 0.88 * cc)
        zones[zone] = {
            "cloud_cover": cc,
            "solar_potential_index": round(potential, 3),
            "forecast_next_day_cloud_cover": daily_cloud_cover(day + timedelta(days=1), zone, settings.random_seed),
        }
    return {"date": day.isoformat(), "issued_at": datetime.now(timezone.utc).isoformat(), "zones": zones}


def _atomic_write(path: str, write_fn) -> None:
    tmp = path + ".tmp"
    write_fn(tmp)
    os.replace(tmp, path)          # atomic on POSIX filesystems


def drop_day(day: date, settings: Settings, rng: random.Random) -> None:
    tariff_dir = os.path.join(settings.landing_dir, "tariffs")
    weather_dir = os.path.join(settings.landing_dir, "weather")
    os.makedirs(tariff_dir, exist_ok=True)
    os.makedirs(weather_dir, exist_ok=True)

    rows = build_tariff_rows(day, settings, rng)
    tariff_path = os.path.join(tariff_dir, f"tariffs_{day.isoformat()}.csv")

    def _write_csv(p):
        with open(p, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    weather = build_weather(day, settings)
    weather_path = os.path.join(weather_dir, f"weather_{day.isoformat()}.json")

    def _write_json(p):
        with open(p, "w") as fh:
            json.dump(weather, fh, indent=2)

    _atomic_write(weather_path, _write_json)
    _atomic_write(tariff_path, _write_csv)
    FILES_DROPPED.labels(feed="tariffs").inc()
    FILES_DROPPED.labels(feed="weather").inc()
    ROWS_WRITTEN.inc(len(rows))
    LAST_DROP.set(time.time())
    LAST_DROP_DAY.set(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    log.info("daily_files_published", sim_date=day.isoformat(), tariff_file=tariff_path, rows=len(rows),
             weather_file=weather_path, fuel_adjustment=fuel_adjustment(day, settings.random_seed))


def already_dropped(day: date, settings: Settings) -> bool:
    pattern = os.path.join(settings.landing_dir, "tariffs", f"tariffs_{day.isoformat()}*.csv")
    return bool(glob.glob(pattern))


def run(settings: Settings, clock: SimClock, stop) -> None:
    rng = random.Random(settings.random_seed)
    log.info("tariff_simulator_started", sim_now=clock.now().isoformat(),
             drop_offset_sim_minutes=DROP_OFFSET_SIM_MINUTES)
    announced_late: set[date] = set()
    while not stop():
        now = clock.now()
        day = now.date() - timedelta(days=1)          # the day that just finished
        if day >= clock.sim_start.date() and not already_dropped(day, settings):
            delay = delivery_delay_minutes(day, settings)
            due = datetime.combine(now.date(), datetime.min.time(), tzinfo=timezone.utc) + \
                timedelta(minutes=DROP_OFFSET_SIM_MINUTES + delay)
            if now >= due:
                drop_day(day, settings, rng)
            elif delay and day not in announced_late:
                announced_late.add(day)
                log.warning("tariff_feed_delayed", sim_date=day.isoformat(), delay_sim_minutes=delay)
        time.sleep(1)


def main() -> None:
    # Group-writable files: the Airflow user (gid 0) must be able to add corrected versions.
    os.umask(0o002)
    settings = get_settings()
    start_http_server(8001)
    # Baseline for the TariffFeedLate alert: "no file yet" counts from start-up, not from 1970.
    LAST_DROP.set(time.time())
    clock = get_clock(settings, wait=True)
    stopping = {"flag": False}

    def _stop(*_):
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    run(settings, clock, lambda: stopping["flag"])


if __name__ == "__main__":
    main()
