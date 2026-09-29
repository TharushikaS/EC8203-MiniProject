"""foreachBatch sinks of the speed layer.

GridStateSink  (stateful query ``grid_state``)
    Spark keeps the state: deduplicated readings aggregated into 1-hour event-time windows per
    household. Each micro-batch emits only the (household, hour) windows that changed (update
    mode) with their *full* running totals. The sink upserts them into ``rt_household_hourly``
    and then rolls them up inside PostgreSQL:
        -> rt_zone_metrics      (zone x hour: load kW, solar kW, renewable share)
        -> rt_household_daily   (household x day: import / export / peak for provisional billing)
    and evaluates the threshold alerts on the refreshed zone windows.

RawIngestSink  (stateless query ``raw_ingest``)
    Appends every record (valid or not) to the Parquet master dataset, updates data-quality
    counters, writes rejected records to the dead-letter topic and samples traces.

Idempotency: every write is an upsert keyed on its natural key, so if Spark re-executes a
micro-batch after a crash the serving tables end up identical (effectively-once). The raw
archive is append-only and therefore at-least-once under replay; the batch layer dedups on
(meter_id, interval_start), which makes a replayed micro-batch harmless.
"""
from __future__ import annotations

import json
from collections import Counter as PyCounter
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from smartgrid.common import db
from smartgrid.common.config import Settings
from smartgrid.common.households import GRID_ZONES, build_registry
from smartgrid.common.logging_utils import get_logger
from smartgrid.common.sim_clock import SimClock
from smartgrid.speed import metrics as M
from smartgrid.speed.transforms import RAW_COLUMNS

log = get_logger("spark-speed-layer", "storage", name="speed.sinks")

DAYLIGHT_ALERT_HOURS = range(9, 16)   # renewable share is only meaningful while the sun is up


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


ZONE_ROLLUP_SQL = """
SELECT grid_zone, window_start, sum(consumption_kwh) AS consumption_kwh, sum(solar_kwh) AS solar_kwh,
       sum(reading_count) AS reading_count, count(*) AS active_meters
FROM rt_household_hourly
WHERE window_start = ANY(%s)
GROUP BY grid_zone, window_start
"""

HOUSEHOLD_DAILY_ROLLUP_SQL = """
INSERT INTO rt_household_daily (sim_date, household_id, grid_zone, consumption_kwh, solar_kwh, import_kwh,
                                export_kwh, peak_import_kwh, reading_count, last_event_ts, updated_at,
                                micro_batch_id)
SELECT (window_start AT TIME ZONE 'UTC')::date, household_id, max(grid_zone), sum(consumption_kwh), sum(solar_kwh),
       sum(import_kwh), sum(export_kwh), sum(peak_import_kwh), sum(reading_count), max(last_event_ts), now(), %s
FROM rt_household_hourly
WHERE window_start >= %s AND window_start < %s
GROUP BY 1, 2
ON CONFLICT (household_id, sim_date) DO UPDATE SET
    consumption_kwh = EXCLUDED.consumption_kwh, solar_kwh = EXCLUDED.solar_kwh, import_kwh = EXCLUDED.import_kwh,
    export_kwh = EXCLUDED.export_kwh, peak_import_kwh = EXCLUDED.peak_import_kwh,
    reading_count = EXCLUDED.reading_count, last_event_ts = EXCLUDED.last_event_ts,
    updated_at = EXCLUDED.updated_at, micro_batch_id = EXCLUDED.micro_batch_id
"""


class GridStateSink:
    """Household-hour windows -> household/zone rollups + LOW_RENEWABLE / ZONE_OVERLOAD alerts."""

    def __init__(self, settings: Settings, clock: SimClock):
        self.s = settings
        self.clock = clock
        registry = build_registry(settings.num_households, settings.random_seed)
        self.meters_per_zone = {z: sum(1 for h in registry if h.grid_zone == z) for z in GRID_ZONES}
        self.interval_h = settings.interval_minutes / 60.0

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        rows = batch_df.collect()
        if not rows:
            return
        now = datetime.now(timezone.utc)
        hh_rows = [(
            r["household_id"], _utc(r["window"]["start"]), r["grid_zone"], round(r["consumption_kwh"], 4),
            round(r["solar_kwh"], 4), round(r["import_kwh"], 4), round(r["export_kwh"], 4),
            round(r["peak_import_kwh"], 4), int(r["reading_count"]), _utc(r["last_event_ts"]), now, batch_id,
        ) for r in rows]
        windows = sorted({row[1] for row in hh_rows})
        try:
            with db.transaction(self.s) as cur:
                db.upsert(cur, "rt_household_hourly",
                          ["household_id", "window_start", "grid_zone", "consumption_kwh", "solar_kwh", "import_kwh",
                           "export_kwh", "peak_import_kwh", "reading_count", "last_event_ts", "updated_at",
                           "micro_batch_id"],
                          hh_rows, conflict_cols=["household_id", "window_start"])
                cur.execute(ZONE_ROLLUP_SQL, (windows,))
                zone_rows, alerts = self._zone_rows(cur.fetchall(), now, batch_id)
                db.upsert(cur, "rt_zone_metrics",
                          ["grid_zone", "window_start", "window_end", "consumption_kwh", "solar_kwh", "load_kw",
                           "solar_kw", "renewable_share", "reading_count", "active_meters", "expected_meters",
                           "intervals_observed", "window_start_real", "updated_at", "micro_batch_id"],
                          zone_rows, conflict_cols=["grid_zone", "window_start"])
                day_start = min(windows).replace(hour=0, minute=0, second=0, microsecond=0)
                day_end = max(windows).replace(hour=0, minute=0, second=0, microsecond=0)
                cur.execute(HOUSEHOLD_DAILY_ROLLUP_SQL,
                            (batch_id, day_start, day_end + timedelta(days=1)))
                new_alerts = []
                for a in alerts:
                    cur.execute(
                        """INSERT INTO grid_alerts (alert_type, grid_zone, window_start, severity, metric_value,
                                                    threshold, message, window_start_real)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (alert_type, grid_zone, window_start) DO NOTHING RETURNING alert_id""",
                        (*a, datetime.fromtimestamp(self.clock.to_real_epoch(a[2]), tz=timezone.utc)),
                    )
                    if cur.fetchone():
                        new_alerts.append(a)
            M.SINK_ROWS.labels(table="rt_household_hourly").inc(len(hh_rows))
            M.SINK_ROWS.labels(table="rt_zone_metrics").inc(len(zone_rows))
        except Exception:
            M.SINK_ERRORS.labels(table="rt_zone_metrics").inc()
            log.exception("sink_write_failed", table="rt_household_hourly/rt_zone_metrics", batch_id=batch_id)
            raise
        if new_alerts:
            self._publish_alerts(batch_df, new_alerts)

    def _zone_rows(self, agg_rows, now: datetime, batch_id: int):
        out, alerts = [], []
        for zone, ws, cons, solar, readings, meters in agg_rows:
            ws = _utc(ws)
            intervals_seen = readings / max(meters, 1)
            hours_observed = max(intervals_seen * self.interval_h, self.interval_h)
            load_kw, solar_kw = cons / hours_observed, solar / hours_observed
            share = min(solar, cons) / cons if cons else 0.0
            expected = self.meters_per_zone.get(zone, 0)
            out.append((zone, ws, ws + timedelta(hours=1),
                        round(cons, 4), round(solar, 4), round(load_kw, 3), round(solar_kw, 3), round(share, 4),
                        int(readings), int(meters), expected, round(intervals_seen, 2),
                        datetime.fromtimestamp(self.clock.to_real_epoch(ws), tz=timezone.utc), now, batch_id))
            # Alert only once a window holds >= 30 simulated minutes of data, to avoid firing on
            # the first readings of a fresh window.
            if intervals_seen >= 2:
                if ws.hour in DAYLIGHT_ALERT_HOURS and share < self.s.low_renewable_threshold:
                    alerts.append(("LOW_RENEWABLE", zone, ws, "warning", share, self.s.low_renewable_threshold,
                                   f"Renewable contribution {share:.0%} in {zone} is below "
                                   f"{self.s.low_renewable_threshold:.0%} during daylight"))
                capacity = expected * self.s.zone_capacity_kw_per_household
                if capacity and load_kw > capacity:
                    alerts.append(("ZONE_OVERLOAD", zone, ws, "critical", load_kw, capacity,
                                   f"Load {load_kw:.0f} kW in {zone} exceeds planned capacity {capacity:.0f} kW"))
        return out, alerts

    def _publish_alerts(self, batch_df: DataFrame, alerts: list[tuple]) -> None:
        """Only NEW alerts are published to Kafka (grid-alerts) for downstream consumers."""
        payloads = []
        for a in alerts:
            M.ALERTS_RAISED.labels(type=a[0]).inc()
            body = {"alert_type": a[0], "grid_zone": a[1], "window_start": a[2].isoformat(), "severity": a[3],
                    "metric_value": round(a[4], 4), "threshold": a[5], "message": a[6]}
            payloads.append((a[1], json.dumps(body)))
            log.warning("grid_alert_raised", **body)
        (batch_df.sparkSession.createDataFrame(payloads, "key string, value string")
         .write.format("kafka")
         .option("kafka.bootstrap.servers", self.s.kafka_bootstrap)
         .option("topic", self.s.topic_alerts)
         .save())


class RawIngestSink:
    """Per micro-batch: raw archive append, data-quality counters, dead-letter queue, sampled traces."""

    def __init__(self, settings: Settings):
        self.s = settings

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        df = batch_df.persist()
        try:
            # 1) Immutable master dataset for the batch layer - every record, valid or not.
            (df.select(*RAW_COLUMNS).withColumn("ingest_batch_id", F.lit(batch_id))
             .write.mode("append").partitionBy("event_date").parquet(self.s.raw_readings_path))

            counts = {(r["invalid_reason"], r["is_late"]): r["n"] for r in
                      df.groupBy("invalid_reason", "is_late").agg(F.count("*").alias("n")).collect()}
            if not counts:
                return
            reasons = PyCounter()
            valid = late = 0
            for (reason, is_late), n in counts.items():
                if reason is None:
                    valid += n
                    late += n if is_late else 0
                else:
                    reasons[reason] += n
            invalid = sum(reasons.values())
            M.RECORDS.labels(status="valid").inc(valid)
            M.RECORDS.labels(status="invalid").inc(invalid)
            M.LATE.inc(late)
            for reason, n in reasons.items():
                M.INVALID.labels(reason=reason).inc(n)

            if invalid:
                # 2) Dead-letter queue keeps the original payload + why it was rejected.
                (df.where(F.col("invalid_reason").isNotNull())
                 .select(F.col("household_id").alias("key"),
                         F.to_json(F.struct("invalid_reason", "raw_value", "kafka_partition", "kafka_offset",
                                            "kafka_ts")).alias("value"))
                 .write.format("kafka")
                 .option("kafka.bootstrap.servers", self.s.kafka_bootstrap)
                 .option("topic", self.s.topic_dlq)
                 .save())

            # 3) Sampled tracing: follow ~TRACE_SAMPLE_PCT % of events through every stage.
            traces = (df.where(F.col("invalid_reason").isNull() &
                               ((F.abs(F.hash("event_id")) % 100) < self.s.trace_sample_pct))
                      .select("event_id", "household_id", "grid_zone", "event_ts", "produced_ts", "kafka_ts",
                              "kafka_partition", "kafka_offset", "is_late")
                      .collect())
            processed_at = datetime.now(timezone.utc)
            trace_rows = []
            for t in traces:
                produced = _utc(t["produced_ts"]) if t["produced_ts"] else None
                latency_ms = (processed_at - produced).total_seconds() * 1000 if produced else None
                if latency_ms is not None and not t["is_late"]:
                    M.E2E_LATENCY.observe(latency_ms / 1000.0)
                trace_rows.append((t["event_id"], t["household_id"], t["grid_zone"], _utc(t["event_ts"]), produced,
                                   _utc(t["kafka_ts"]), int(t["kafka_partition"]), int(t["kafka_offset"]),
                                   processed_at, latency_ms, bool(t["is_late"]), batch_id))
            with db.transaction(self.s) as cur:
                db.upsert(cur, "pipeline_event_trace",
                          ["event_id", "household_id", "grid_zone", "event_ts", "produced_at", "kafka_ts",
                           "kafka_partition", "kafka_offset", "speed_processed_at", "e2e_latency_ms", "is_late",
                           "micro_batch_id"], trace_rows, conflict_cols=["event_id"], update_cols=[])
                cur.execute(
                    """INSERT INTO stream_quality_stats (micro_batch_id, processed_at, valid_count, invalid_count,
                                                         late_count, invalid_reasons)
                       VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (micro_batch_id) DO NOTHING""",
                    (batch_id, processed_at, valid, invalid, late, json.dumps(dict(reasons))),
                )
            M.SINK_ROWS.labels(table="raw_archive").inc(valid + invalid)
            M.SINK_ROWS.labels(table="pipeline_event_trace").inc(len(trace_rows))
            log.info("raw_batch_ingested", batch_id=batch_id, valid=valid, invalid=invalid, late=late,
                     invalid_reasons=dict(reasons), traces_sampled=len(trace_rows))
        except Exception:
            M.SINK_ERRORS.labels(table="raw_archive").inc()
            log.exception("raw_ingest_failed", batch_id=batch_id)
            raise
        finally:
            df.unpersist()
