"""BATCH LAYER - authoritative daily billing & solar-contribution views (Spark batch job).

    spark-submit batch_billing.py --start-date 2026-03-02 [--end-date ...] --run-id <id> [--run-type daily|recompute]

For every simulated day in the range the job recomputes, from the immutable raw archive:
  1. Validation   - same rules as the speed layer (shared ``transforms`` module).
  2. Dedup        - exact: one reading per (meter_id, interval_start). Unlike the speed
                    layer's watermark-bounded dedup this sees the whole day, including
                    late outage flushes.
  3. Enrichment   - join with the validated daily tariff (broadcast) and weather.
  4. Pricing      - ``common.billing.compute_bill`` applied via ``mapInPandas`` (the exact
                    function the serving layer uses for provisional speed-layer estimates).
  5. Aggregation  - household daily bills, zone daily summary, zone x hour-of-day profile.
  6. Publish      - idempotent replace of the date range in PostgreSQL (staging table +
                    single transaction) and dynamic partition overwrite of the curated Parquet.

Because the job is a pure function of (raw data, reference data, code), re-running it for
any date always gives the same answer - which is what makes corrections and backfills safe.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from smartgrid.common import db
from smartgrid.common.billing import compute_bill
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.logging_utils import get_logger
from smartgrid.speed import transforms as TX

log = get_logger("batch-layer", "processing", name="batch.billing")

BILL_SCHEMA = (
    "bill_date date, household_id string, grid_zone string, tariff_plan string, billing_tier string, "
    "subsidy_flag boolean, tariff_version int, carried_forward boolean, consumption_kwh double, "
    "solar_kwh double, self_consumed_kwh double, import_kwh double, peak_import_kwh double, "
    "export_kwh double, tariff_rate double, feed_in_rate double, energy_charge double, "
    "block_surcharge double, fixed_charge double, subsidy_discount double, feed_in_credit double, "
    "net_amount double, amount_due double, carried_credit double, reading_count long, "
    "late_readings long, data_completeness double"
)


def price_partitions(batches):
    """mapInPandas UDF: price each household-day with the shared billing rules."""
    for pdf in batches:
        if pdf.empty:
            yield pdf.reindex(columns=[c.split()[0] for c in BILL_SCHEMA.split(", ")])
            continue
        priced = [compute_bill(
            import_kwh=r.import_kwh, peak_import_kwh=r.peak_import_kwh, export_kwh=r.export_kwh,
            tariff_rate=r.tariff_rate, tariff_plan=r.tariff_plan, billing_tier=r.billing_tier,
            subsidy_flag=bool(r.subsidy_flag), feed_in_rate=r.feed_in_rate,
        ).to_dict() for r in pdf.itertuples()]
        out = pd.concat([pdf.reset_index(drop=True), pd.DataFrame(priced)], axis=1)
        yield out[[c.split()[0] for c in BILL_SCHEMA.split(", ")]]


def build_spark(app: str = "smartgrid-batch-billing") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def available_dates(settings: Settings, start: date, end: date) -> list[date]:
    days, d = [], start
    while d <= end:
        if os.path.isdir(os.path.join(settings.raw_readings_path, f"event_date={d.isoformat()}")):
            days.append(d)
        d += timedelta(days=1)
    return days


def compute_views(spark: SparkSession, settings: Settings, start: date, end: date) -> dict[str, DataFrame]:
    s_lit, e_lit = F.lit(start.isoformat()).cast("date"), F.lit(end.isoformat()).cast("date")

    raw = spark.read.parquet(settings.raw_readings_path).where(F.col("event_date").between(s_lit, e_lit))
    checked = TX.add_quality_columns(raw, settings.watermark_sim_minutes).cache()

    valid = checked.where(F.col("invalid_reason").isNull())
    first = Window.partitionBy("meter_id", "interval_start_ts").orderBy("kafka_ts", "kafka_offset")
    dedup = valid.withColumn("_rn", F.row_number().over(first)).where("_rn = 1").drop("_rn")
    energy = TX.add_interval_energy(dedup).cache()

    quality = (
        checked.groupBy("event_date").agg(
            F.count("*").alias("raw_records"),
            F.sum(F.col("invalid_reason").isNotNull().cast("int")).alias("invalid_records"),
        ).join(
            energy.groupBy("event_date").agg(
                F.count("*").alias("valid_records"),
                F.sum(F.col("is_late").cast("int")).alias("late_records")),
            "event_date", "left")
        .withColumn("duplicates_removed", F.col("raw_records") - F.col("invalid_records") - F.col("valid_records"))
    )
    invalid_by_reason = (checked.where(F.col("invalid_reason").isNotNull())
                         .groupBy("event_date", "invalid_reason").count())

    hh = energy.groupBy(F.col("event_date").alias("bill_date"), "household_id", "grid_zone").agg(
        F.sum("power_consumption_kwh").alias("consumption_kwh"),
        F.sum("solar_generation_kwh").alias("solar_kwh"),
        F.sum("self_consumed_kwh").alias("self_consumed_kwh"),
        F.sum("import_kwh").alias("import_kwh"),
        F.sum("peak_import_kwh").alias("peak_import_kwh"),
        F.sum("export_kwh").alias("export_kwh"),
        F.countDistinct("interval_start_ts").alias("reading_count"),
        F.sum(F.col("is_late").cast("long")).alias("late_readings"),
    ).withColumn("data_completeness", F.round(F.col("reading_count") / F.lit(settings.readings_per_day), 4))

    tariffs = (spark.read.parquet(os.path.join(settings.reference_dir, "tariffs"))
               .where(F.col("bill_date").between(s_lit, e_lit)))
    joined = hh.join(F.broadcast(tariffs), ["bill_date", "household_id"], "left")
    no_tariff = joined.where(F.col("tariff_rate").isNull()).groupBy("bill_date").count()
    bills = (joined.where(F.col("tariff_rate").isNotNull())
             .select("bill_date", "household_id", "grid_zone", "tariff_plan", "billing_tier", "subsidy_flag",
                     F.col("tariff_version").cast("int").alias("tariff_version"), "carried_forward",
                     "consumption_kwh", "solar_kwh", "self_consumed_kwh", "import_kwh", "peak_import_kwh",
                     "export_kwh", "tariff_rate", "feed_in_rate", "reading_count", "late_readings",
                     "data_completeness")
             .mapInPandas(price_partitions, BILL_SCHEMA)
             .cache())

    hourly = (energy.groupBy(F.col("event_date").alias("bill_date"), "hour_of_day", "grid_zone")
              .agg(F.sum("power_consumption_kwh").alias("consumption_kwh"),
                   F.sum("solar_generation_kwh").alias("solar_kwh"))
              .withColumn("renewable_share", F.round(TX.renewable_share(F.col("solar_kwh"), F.col("consumption_kwh")), 4)))

    peak = (hourly.withColumn("_r", F.row_number().over(
                Window.partitionBy("bill_date", "grid_zone").orderBy(F.desc("consumption_kwh"))))
            .where("_r = 1").select("bill_date", "grid_zone", F.col("consumption_kwh").alias("peak_load_kw"),
                                    F.col("hour_of_day").alias("peak_hour")))
    weather_path = os.path.join(settings.reference_dir, "weather")
    zone = (bills.groupBy("bill_date", "grid_zone").agg(
                F.countDistinct("household_id").alias("households"),
                F.sum("consumption_kwh").alias("consumption_kwh"),
                F.sum("solar_kwh").alias("solar_kwh"),
                F.sum("export_kwh").alias("export_kwh"),
                F.sum("amount_due").alias("total_billed"),
                F.avg("amount_due").alias("avg_bill"),
                F.sum("feed_in_credit").alias("total_feed_in_credit"))
            .withColumn("renewable_share", F.round(TX.renewable_share(F.col("solar_kwh"), F.col("consumption_kwh")), 4))
            .join(peak, ["bill_date", "grid_zone"], "left"))
    if os.path.isdir(weather_path):
        weather = spark.read.parquet(weather_path).where(F.col("bill_date").between(s_lit, e_lit)) \
            .select("bill_date", "grid_zone", "cloud_cover")
        zone = zone.join(F.broadcast(weather), ["bill_date", "grid_zone"], "left")
    else:
        zone = zone.withColumn("cloud_cover", F.lit(None).cast("double"))

    return {"bills": bills, "zone": zone, "hourly": hourly, "quality": quality,
            "invalid_by_reason": invalid_by_reason, "no_tariff": no_tariff}


def _replace_range(df: DataFrame, table: str, columns: list[str], start: date, end: date,
                   run_id: str, settings: Settings) -> int:
    """Write df to a staging table via JDBC, then swap it into ``table`` for the date range
    inside ONE transaction - readers never see a half-written day, re-runs are idempotent."""
    staging = f"stg_{table}_{re.sub(r'[^a-z0-9]', '_', run_id.lower())[-40:]}"
    out = df.withColumn("batch_run_id", F.lit(run_id)).withColumn("computed_at", F.current_timestamp())
    cols = columns + ["batch_run_id", "computed_at"]
    (out.select(*cols).write.format("jdbc")
     .option("url", settings.jdbc_url).option("dbtable", staging)
     .option("user", settings.postgres_user).option("password", settings.postgres_password)
     .option("driver", "org.postgresql.Driver").mode("overwrite").save())
    with db.transaction(settings) as cur:
        cur.execute(f"DELETE FROM {table} WHERE bill_date BETWEEN %s AND %s", (start, end))
        cur.execute(f"INSERT INTO {table} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM {staging}")
        n = cur.rowcount
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
    return n


def run(start: date, end: date, run_id: str, run_type: str, settings: Settings | None = None) -> list[dict]:
    settings = settings or get_settings()
    t0 = time.time()
    days = available_dates(settings, start, end)
    if not days:
        raise RuntimeError(f"No raw data partitions between {start} and {end}")
    start, end = min(days), max(days)
    log.info("batch_billing_started", start=str(start), end=str(end), run_id=run_id, run_type=run_type)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        views = compute_views(spark, settings, start, end)
        bill_cols = [c.split()[0] for c in BILL_SCHEMA.split(", ")]
        n_bills = _replace_range(views["bills"], "batch_household_daily_bill", bill_cols, start, end, run_id, settings)
        n_zone = _replace_range(views["zone"], "batch_zone_daily_summary",
                                ["bill_date", "grid_zone", "households", "consumption_kwh", "solar_kwh", "export_kwh",
                                 "renewable_share", "peak_load_kw", "peak_hour", "cloud_cover", "total_billed",
                                 "avg_bill", "total_feed_in_credit"], start, end, run_id, settings)
        n_hourly = _replace_range(views["hourly"], "batch_zone_hourly",
                                  ["bill_date", "hour_of_day", "grid_zone", "consumption_kwh", "solar_kwh",
                                   "renewable_share"], start, end, run_id, settings)
        (views["bills"].write.mode("overwrite").partitionBy("bill_date")
         .parquet(os.path.join(settings.curated_dir, "household_daily_bill")))

        quality = {r["event_date"]: r.asDict() for r in views["quality"].collect()}
        reasons: dict = {}
        for r in views["invalid_by_reason"].collect():
            reasons.setdefault(r["event_date"], {})[r["invalid_reason"]] = r["count"]
        no_tariff = {r["bill_date"]: r["count"] for r in views["no_tariff"].collect()}
        billed = {r["bill_date"]: (r["n"], r["total"]) for r in
                  views["bills"].groupBy("bill_date").agg(F.count("*").alias("n"),
                                                         F.sum("amount_due").alias("total")).collect()}
        tariff_versions = {r["bill_date"]: r["v"] for r in
                           views["bills"].groupBy("bill_date").agg(F.max("tariff_version").alias("v")).collect()}
        duration = round(time.time() - t0, 2)
        summaries = []
        finished = datetime.now(timezone.utc)
        with db.transaction(settings) as cur:
            for d in days:
                q = quality.get(d, {})
                n, total = billed.get(d, (0, 0.0))
                row = {
                    "run_id": run_id, "bill_date": d, "run_type": run_type, "status": "SUCCESS",
                    "raw_records": int(q.get("raw_records") or 0), "invalid_records": int(q.get("invalid_records") or 0),
                    "duplicates_removed": int(q.get("duplicates_removed") or 0),
                    "valid_records": int(q.get("valid_records") or 0), "late_records": int(q.get("late_records") or 0),
                    "households_billed": int(n), "households_without_tariff": int(no_tariff.get(d, 0)),
                    "total_billed": round(float(total or 0.0), 2), "tariff_version": tariff_versions.get(d),
                    "invalid_reasons": reasons.get(d, {}), "duration_seconds": duration, "finished_at": finished,
                }
                cur.execute(
                    """INSERT INTO batch_runs (run_id, bill_date, run_type, status, raw_records, invalid_records,
                           duplicates_removed, valid_records, late_records, households_billed,
                           households_without_tariff, total_billed, tariff_version, invalid_reasons,
                           duration_seconds, started_at, finished_at)
                       VALUES (%(run_id)s, %(bill_date)s, %(run_type)s, %(status)s, %(raw_records)s,
                           %(invalid_records)s, %(duplicates_removed)s, %(valid_records)s, %(late_records)s,
                           %(households_billed)s, %(households_without_tariff)s, %(total_billed)s,
                           %(tariff_version)s, %(invalid_reasons_json)s, %(duration_seconds)s,
                           to_timestamp(%(t0)s), %(finished_at)s)
                       ON CONFLICT (run_id, bill_date) DO UPDATE SET status = EXCLUDED.status,
                           raw_records = EXCLUDED.raw_records, invalid_records = EXCLUDED.invalid_records,
                           duplicates_removed = EXCLUDED.duplicates_removed, valid_records = EXCLUDED.valid_records,
                           late_records = EXCLUDED.late_records, households_billed = EXCLUDED.households_billed,
                           households_without_tariff = EXCLUDED.households_without_tariff,
                           total_billed = EXCLUDED.total_billed, tariff_version = EXCLUDED.tariff_version,
                           invalid_reasons = EXCLUDED.invalid_reasons, duration_seconds = EXCLUDED.duration_seconds,
                           finished_at = EXCLUDED.finished_at, error = NULL""",
                    {**row, "invalid_reasons_json": json.dumps(row["invalid_reasons"]), "t0": t0},
                )
                summaries.append({**row, "bill_date": d.isoformat(), "finished_at": finished.isoformat()})
                log.info("batch_day_published", **{k: v for k, v in summaries[-1].items() if k != "status"})
        log.info("batch_billing_finished", run_id=run_id, days=len(days), bills=n_bills, zone_rows=n_zone,
                 hourly_rows=n_hourly, duration_seconds=duration)
        return summaries
    finally:
        spark.stop()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--start-date", required=True, type=date.fromisoformat)
    p.add_argument("--end-date", type=date.fromisoformat)
    p.add_argument("--run-id", default=f"manual_{int(time.time())}")
    p.add_argument("--run-type", default="daily", choices=["daily", "recompute", "manual"])
    a = p.parse_args()
    run(a.start_date, a.end_date or a.start_date, a.run_id, a.run_type)


if __name__ == "__main__":
    main()
