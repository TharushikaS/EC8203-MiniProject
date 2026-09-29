"""Spark transformations shared by the speed layer and the batch layer.

Lambda architectures are often criticised for duplicating logic in two code bases.
We mitigate that by putting parsing, validation and per-interval energy maths here and
calling the *same* functions from the streaming job and from the batch billing job.
The layers differ only in what they do afterwards (windowed approximations vs. a full,
deduplicated recomputation).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from smartgrid.common.billing import PEAK_HOURS
from smartgrid.common.households import GRID_ZONES
from smartgrid.common.sim_clock import SimClock

MAX_INTERVAL_KWH = 10.0            # > 40 kW average over 15 min is physically implausible for a home
FUTURE_TOLERANCE_SIM_MINUTES = 60  # readings stamped more than 1 simulated hour ahead are clock skew

METER_EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("meter_id", T.StringType()),
    T.StructField("household_id", T.StringType()),
    T.StructField("grid_zone", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("interval_minutes", T.IntegerType()),
    T.StructField("power_consumption_kwh", T.DoubleType()),
    T.StructField("solar_generation_kwh", T.DoubleType()),
    T.StructField("voltage_v", T.DoubleType()),
    T.StructField("produced_at", T.StringType()),
])

# Columns persisted in the immutable raw archive (the Lambda "master dataset").
RAW_COLUMNS = [
    "event_id", "schema_version", "meter_id", "household_id", "grid_zone", "event_ts",
    "interval_start_ts", "interval_minutes", "power_consumption_kwh", "solar_generation_kwh",
    "voltage_v", "produced_ts", "kafka_ts", "kafka_partition", "kafka_offset", "sim_ingest_ts",
    "raw_value", "event_date",
]


def sim_time_of(real_ts: Column, clock: SimClock) -> Column:
    """Map a real timestamp column to simulated time (same formula as SimClock.now)."""
    real_epoch = real_ts.cast("double")
    return F.timestamp_seconds(F.lit(clock.sim_start_epoch) + (real_epoch - F.lit(clock.anchor_real)) * F.lit(clock.speed))


def real_time_of(sim_ts: Column, clock: SimClock) -> Column:
    sim_epoch = sim_ts.cast("double")
    return F.timestamp_seconds(F.lit(clock.anchor_real) + (sim_epoch - F.lit(clock.sim_start_epoch)) / F.lit(clock.speed))


def parse_kafka_records(kafka_df: DataFrame, clock: SimClock) -> DataFrame:
    """Kafka rows -> typed meter readings (malformed payloads keep null fields + raw_value)."""
    parsed = (
        kafka_df.select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("j", F.from_json("raw_value", METER_EVENT_SCHEMA))
        .select("*", "j.*")
        .drop("j", "timestamp")
        .withColumn("event_ts", F.to_timestamp(F.get_json_object("raw_value", "$.timestamp")))
        .withColumn("produced_ts", F.to_timestamp("produced_at"))
        .withColumn("interval_minutes", F.coalesce(F.col("interval_minutes"), F.lit(15)))
        # Interval START is the event time used for windows, dates and hours: the reading
        # stamped 00:00 covers 23:45-00:00 and belongs to the previous day.
        .withColumn("interval_start_ts",
                    F.timestamp_seconds(F.col("event_ts").cast("long") - F.col("interval_minutes") * 60))
        .withColumn("sim_ingest_ts", sim_time_of(F.col("kafka_ts"), clock))
    )
    return parsed.withColumn(
        "event_date", F.coalesce(F.to_date("interval_start_ts"), F.to_date("sim_ingest_ts"))
    )


def invalid_reason_col() -> Column:
    """First failing validation rule, or null if the reading is valid."""
    cons, solar = F.col("power_consumption_kwh"), F.col("solar_generation_kwh")
    future_limit = F.col("sim_ingest_ts") + F.expr(f"INTERVAL {FUTURE_TOLERANCE_SIM_MINUTES} MINUTES")
    return (
        F.when(F.col("event_id").isNull(), F.lit("malformed_json"))
        .when(F.col("household_id").isNull() | F.col("meter_id").isNull(), F.lit("missing_household"))
        .when(F.col("event_ts").isNull(), F.lit("bad_timestamp"))
        .when(F.col("grid_zone").isNull() | ~F.col("grid_zone").isin(*GRID_ZONES), F.lit("unknown_zone"))
        .when(cons.isNull() | solar.isNull() | (cons < 0) | (solar < 0), F.lit("negative_kwh"))
        .when((cons > MAX_INTERVAL_KWH) | (solar > MAX_INTERVAL_KWH), F.lit("impossible_kwh"))
        .when(F.col("event_ts") > future_limit, F.lit("future_timestamp"))
    )


def add_quality_columns(df: DataFrame, watermark_sim_minutes: int) -> DataFrame:
    """Adds invalid_reason, ingest delay (simulated minutes) and the is_late flag.

    is_late marks readings that reached Kafka later than the speed layer's watermark
    allows - the speed layer drops them, the batch layer includes them.
    """
    delay_min = (F.col("sim_ingest_ts").cast("double") - F.col("event_ts").cast("double")) / 60.0
    return (
        df.withColumn("invalid_reason", invalid_reason_col())
        .withColumn("ingest_delay_sim_minutes", F.round(delay_min, 1))
        .withColumn("is_late", delay_min > F.lit(float(watermark_sim_minutes)))
    )


def add_interval_energy(df: DataFrame) -> DataFrame:
    """Per-interval import/export split. Solar is consumed on-site first; the rest is exported."""
    cons, solar = F.col("power_consumption_kwh"), F.col("solar_generation_kwh")
    hour = F.hour("interval_start_ts")
    return (
        df.withColumn("import_kwh", F.greatest(cons - solar, F.lit(0.0)))
        .withColumn("export_kwh", F.greatest(solar - cons, F.lit(0.0)))
        .withColumn("self_consumed_kwh", F.least(cons, solar))
        .withColumn("hour_of_day", hour)
        .withColumn("is_peak", hour.isin(*sorted(PEAK_HOURS)))
        .withColumn("peak_import_kwh", F.when(F.col("is_peak"), F.col("import_kwh")).otherwise(F.lit(0.0)))
    )


def renewable_share(solar: Column, consumption: Column) -> Column:
    """Share of load met by local solar: min(solar, load) / load, 0 when load is 0."""
    return F.when(consumption > 0, F.least(solar, consumption) / consumption).otherwise(F.lit(0.0))
