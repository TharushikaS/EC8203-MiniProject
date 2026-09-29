"""Spark-level tests for the transformations shared by the speed and batch layers."""
import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from smartgrid.batch.batch_billing import BILL_SCHEMA, price_partitions  # noqa: E402
from smartgrid.common.billing import compute_bill  # noqa: E402
from smartgrid.common.sim_clock import SimClock  # noqa: E402
from smartgrid.speed import transforms as TX  # noqa: E402

START = datetime(2026, 3, 1, tzinfo=timezone.utc)
# anchor chosen so that real 2026-03-01 00:00 == sim 2026-03-01 00:00 and speed 288x
CLOCK = SimClock(anchor_real=START.timestamp(), sim_start=START, sim_day_seconds=300)


def reading(event_id="e1", ts="2026-03-01T12:15:00Z", cons=0.5, solar=0.2, hh="HH-00001", zone="GALLE"):
    return json.dumps({"event_id": event_id, "schema_version": 1, "meter_id": "MTR-00001", "household_id": hh,
                       "grid_zone": zone, "timestamp": ts, "interval_minutes": 15,
                       "power_consumption_kwh": cons, "solar_generation_kwh": solar,
                       "produced_at": "2026-03-01T00:02:40Z"})


def kafka_df(spark, values, kafka_ts=datetime(2026, 3, 1, 0, 2, 40, tzinfo=timezone.utc)):
    # kafka_ts 00:02:40 real == sim 12:48 on 2026-03-01 (160 s * 288)
    rows = [(v.encode() if isinstance(v, str) else v, 0, i, kafka_ts) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, "value binary, partition int, offset long, timestamp timestamp")


def checked(spark, values, **kw):
    parsed = TX.parse_kafka_records(kafka_df(spark, values, **kw), CLOCK)
    return TX.add_quality_columns(parsed, watermark_sim_minutes=120)


def test_valid_reading_parses_with_interval_start_as_event_time(spark):
    r = checked(spark, [reading()]).collect()[0]
    assert r["invalid_reason"] is None
    assert r["interval_start_ts"].strftime("%H:%M") == "12:00"
    assert str(r["event_date"]) == "2026-03-01"
    assert not r["is_late"]


def test_midnight_reading_belongs_to_previous_day(spark):
    r = checked(spark, [reading(ts="2026-03-02T00:00:00Z")],
                kafka_ts=datetime(2026, 3, 1, 0, 5, 1, tzinfo=timezone.utc)).collect()[0]
    assert str(r["event_date"]) == "2026-03-01"


@pytest.mark.parametrize("payload,reason", [
    ("{broken", "malformed_json"),
    (reading(hh=None), "missing_household"),
    (reading(cons=-1.0), "negative_kwh"),
    (reading(cons=99.0), "impossible_kwh"),
    (reading(zone="ATLANTIS"), "unknown_zone"),
    (reading(ts="2026-03-09T12:15:00Z"), "future_timestamp"),
])
def test_invalid_readings_get_a_reason(spark, payload, reason):
    assert checked(spark, [payload]).collect()[0]["invalid_reason"] == reason


def test_readings_older_than_watermark_are_flagged_late(spark):
    # event at 08:00 sim, ingested at 12:48 sim -> 4.8 h late > 2 h watermark
    r = checked(spark, [reading(ts="2026-03-01T08:00:00Z")]).collect()[0]
    assert r["is_late"] and r["invalid_reason"] is None


def test_interval_energy_split_import_export(spark):
    df = TX.add_interval_energy(checked(spark, [reading(cons=0.5, solar=0.8)]))
    r = df.collect()[0]
    assert r["import_kwh"] == 0 and r["export_kwh"] == pytest.approx(0.3) and r["self_consumed_kwh"] == 0.5


def test_batch_pricing_udf_matches_python_billing_function(spark):
    """The batch layer (mapInPandas) and the serving layer (python) must price identically."""
    cols = ("bill_date date, household_id string, grid_zone string, tariff_plan string, billing_tier string, "
            "subsidy_flag boolean, tariff_version int, carried_forward boolean, consumption_kwh double, "
            "solar_kwh double, self_consumed_kwh double, import_kwh double, peak_import_kwh double, "
            "export_kwh double, tariff_rate double, feed_in_rate double, reading_count long, late_readings long, "
            "data_completeness double")
    rows = [
        (datetime(2026, 3, 1).date(), "HH-1", "GALLE", "TOU", "HIGH_USAGE", True, 1, False, 14.0, 2.0, 1.5,
         12.5, 4.0, 0.5, 31.2, 22.0, 96, 3, 1.0),
        (datetime(2026, 3, 1).date(), "HH-2", "JAFFNA", "GREEN", "LIFELINE", False, 1, False, 3.0, 9.0, 2.5,
         0.5, 0.0, 6.5, 33.1, 27.0, 96, 0, 1.0),
    ]
    out = {r["household_id"]: r for r in
           spark.createDataFrame(rows, cols).mapInPandas(price_partitions, BILL_SCHEMA).collect()}
    for hh, plan, tier, sub, imp, peak, exp, rate, feed in [
        ("HH-1", "TOU", "HIGH_USAGE", True, 12.5, 4.0, 0.5, 31.2, 22.0),
        ("HH-2", "GREEN", "LIFELINE", False, 0.5, 0.0, 6.5, 33.1, 27.0),
    ]:
        expected = compute_bill(import_kwh=imp, peak_import_kwh=peak, export_kwh=exp, tariff_rate=rate,
                                tariff_plan=plan, billing_tier=tier, subsidy_flag=sub, feed_in_rate=feed)
        assert out[hh]["amount_due"] == pytest.approx(expected.amount_due)
        assert out[hh]["carried_credit"] == pytest.approx(expected.carried_credit)


def test_renewable_share_is_capped_at_one(spark):
    df = spark.createDataFrame([(5.0, 2.0), (1.0, 3.0), (0.0, 0.0)], "solar double, cons double")
    shares = [r[0] for r in df.select(TX.renewable_share(F.col("solar"), F.col("cons"))).collect()]
    assert shares == [1.0, pytest.approx(1 / 3), 0.0]
