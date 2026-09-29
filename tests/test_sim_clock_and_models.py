import random
from datetime import date, datetime, timedelta, timezone

from smartgrid.common.energy_model import clear_sky_factor, daily_cloud_cover, interval_solar_kwh
from smartgrid.common.households import GRID_ZONES, build_registry
from smartgrid.common.sim_clock import SimClock, load_or_create_clock


def test_clock_maps_real_to_sim_at_configured_speed():
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    clock = SimClock(anchor_real=1_000_000.0, sim_start=start, sim_day_seconds=300)
    assert clock.speed == 288
    assert clock.now(1_000_300.0) == start + timedelta(days=1)
    assert clock.to_real_epoch(start + timedelta(hours=12)) == 1_000_150.0


def test_clock_file_is_shared_between_services(tmp_path):
    a = load_or_create_clock(str(tmp_path), "2026-03-01T00:00:00+00:00", 300)
    b = load_or_create_clock(str(tmp_path), "2030-01-01T00:00:00+00:00", 60)   # later reader: file wins
    assert a == b


def test_registry_is_deterministic_and_covers_all_zones():
    r1 = build_registry(200, 42)
    r2 = build_registry(200, 42)
    assert r1 == r2
    assert {h.grid_zone for h in r1} == set(GRID_ZONES)
    assert len({h.household_id for h in r1}) == 200


def test_no_solar_at_night_and_peak_near_noon():
    assert clear_sky_factor(2.0) == 0
    assert clear_sky_factor(20.0) == 0
    assert clear_sky_factor(12.25) > 0.99


def test_overcast_days_cut_solar_output():
    h = next(h for h in build_registry(200, 42) if h.has_solar)
    days = [date(2026, 3, 1) + timedelta(days=i) for i in range(60)]
    covers = {d: daily_cloud_cover(d, h.grid_zone, 42) for d in days}
    clear = min(covers, key=covers.get)
    cloudy = max(covers, key=covers.get)
    rng = random.Random(0)
    assert interval_solar_kwh(h, clear, 12.0, 15, 42, rng) > interval_solar_kwh(h, cloudy, 12.0, 15, 42, rng)
