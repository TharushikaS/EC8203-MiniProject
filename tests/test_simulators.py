import json
import random
from datetime import date, datetime, timezone

import pytest

meter = pytest.importorskip("smartgrid.simulators.meter_stream")
from smartgrid.common.households import build_registry  # noqa: E402
from smartgrid.simulators import daily_tariff_drop as tariff  # noqa: E402

REQUIRED = {"event_id", "meter_id", "household_id", "grid_zone", "timestamp", "power_consumption_kwh",
            "solar_generation_kwh", "interval_minutes"}


def test_reading_has_required_fields_and_sane_values(settings):
    h = build_registry(10, 42)[0]
    r = meter.build_reading(h, datetime(2026, 3, 1, 12, 15, tzinfo=timezone.utc), settings, random.Random(1))
    assert REQUIRED <= r.keys()
    assert r["timestamp"].endswith("Z")
    assert 0 <= r["power_consumption_kwh"] < 5
    assert r["solar_generation_kwh"] >= 0


def test_night_readings_have_no_solar(settings):
    h = next(h for h in build_registry(100, 42) if h.has_solar)
    r = meter.build_reading(h, datetime(2026, 3, 1, 2, 0, tzinfo=timezone.utc), settings, random.Random(1))
    assert r["solar_generation_kwh"] == 0


@pytest.mark.parametrize("seed", range(20))
def test_corruptions_are_detectably_invalid(settings, seed):
    h = build_registry(5, 42)[0]
    good = meter.build_reading(h, datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc), settings, random.Random(seed))
    bad, fault = meter.corrupt(good, random.Random(seed))
    if fault == "malformed":
        with pytest.raises(json.JSONDecodeError):
            json.loads(bad)
    elif fault == "negative_kwh":
        assert bad["power_consumption_kwh"] < 0
    elif fault == "impossible_kwh":
        assert bad["power_consumption_kwh"] > 10
    elif fault == "missing_household":
        assert bad["household_id"] is None
    else:
        assert bad["timestamp"] > good["timestamp"]


def test_tariff_file_covers_every_household_and_rates_vary_by_day(settings):
    rows = tariff.build_tariff_rows(date(2026, 3, 1), settings, random.Random(1))
    ids = {r["household_id"] for r in rows}
    assert ids == {h.household_id for h in build_registry(settings.num_households, settings.random_seed)}
    assert tariff.fuel_adjustment(date(2026, 3, 1), 42) != tariff.fuel_adjustment(date(2026, 3, 2), 42)


def test_drop_day_writes_both_feeds_atomically(settings):
    tariff.drop_day(date(2026, 3, 1), settings, random.Random(1))
    assert tariff.already_dropped(date(2026, 3, 1), settings)
    weather = json.load(open(f"{settings.landing_dir}/weather/weather_2026-03-01.json"))
    assert set(weather["zones"]) == {"COLOMBO", "GALLE", "KANDY", "MATARA", "JAFFNA"}
