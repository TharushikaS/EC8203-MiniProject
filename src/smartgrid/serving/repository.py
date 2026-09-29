"""Data access for the serving API (thin SQL layer over the PostgreSQL serving store)."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Any

import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from smartgrid.common.config import Settings


class Repository:
    def __init__(self, settings: Settings, minconn: int = 1, maxconn: int = 8):
        self.settings = settings
        self._pool = ThreadedConnectionPool(minconn, maxconn, settings.postgres_dsn, connect_timeout=5)

    @contextmanager
    def cursor(self):
        conn = self._pool.getconn()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    yield cur
        finally:
            self._pool.putconn(conn)

    def fetchall(self, sql: str, params: Any = None) -> list[dict]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def fetchone(self, sql: str, params: Any = None) -> dict | None:
        rows = self.fetchall(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: Any = None) -> None:
        with self.cursor() as cur:
            cur.execute(sql, params)

    # ------------------------------------------------------------------ speed views
    def latest_zone_metrics(self) -> list[dict]:
        """Latest window per zone that holds at least 2 intervals of data (a window that
        has only just opened would under-report load)."""
        return self.fetchall("""
            SELECT DISTINCT ON (grid_zone) *
            FROM rt_zone_metrics
            WHERE intervals_observed >= 2
            ORDER BY grid_zone, window_start DESC""")

    def zone_history(self, zone: str, limit: int) -> list[dict]:
        return self.fetchall("""SELECT * FROM rt_zone_metrics WHERE grid_zone = %s
                                ORDER BY window_start DESC LIMIT %s""", (zone, limit))

    def alerts(self, limit: int, alert_type: str | None, zone: str | None) -> list[dict]:
        sql = "SELECT * FROM grid_alerts WHERE TRUE"
        params: list[Any] = []
        if alert_type:
            sql += " AND alert_type = %s"
            params.append(alert_type)
        if zone:
            sql += " AND grid_zone = %s"
            params.append(zone)
        sql += " ORDER BY window_start DESC LIMIT %s"
        params.append(limit)
        return self.fetchall(sql, params)

    def household_speed_days(self, household_id: str, since: date) -> list[dict]:
        return self.fetchall("""SELECT * FROM rt_household_daily WHERE household_id = %s AND sim_date >= %s
                                ORDER BY sim_date""", (household_id, since))

    # ------------------------------------------------------------------ batch views
    def household_bills(self, household_id: str, since: date | None = None, limit: int = 31) -> list[dict]:
        if since:
            return self.fetchall("""SELECT * FROM batch_household_daily_bill WHERE household_id = %s
                                    AND bill_date >= %s ORDER BY bill_date""", (household_id, since))
        return self.fetchall("""SELECT * FROM batch_household_daily_bill WHERE household_id = %s
                                ORDER BY bill_date DESC LIMIT %s""", (household_id, limit))

    def latest_tariff(self, household_id: str) -> dict | None:
        return self.fetchone("""SELECT * FROM reference_tariffs WHERE household_id = %s
                                ORDER BY bill_date DESC LIMIT 1""", (household_id,))

    def zone_daily(self, bill_date: date) -> list[dict]:
        return self.fetchall("SELECT * FROM batch_zone_daily_summary WHERE bill_date = %s ORDER BY grid_zone",
                             (bill_date,))

    def bills_for_day(self, bill_date: date, zone: str | None, limit: int, offset: int) -> list[dict]:
        sql = "SELECT * FROM batch_household_daily_bill WHERE bill_date = %s"
        params: list[Any] = [bill_date]
        if zone:
            sql += " AND grid_zone = %s"
            params.append(zone)
        sql += " ORDER BY household_id LIMIT %s OFFSET %s"
        params += [limit, offset]
        return self.fetchall(sql, params)

    def reconciliation(self, bill_date: date) -> list[dict]:
        return self.fetchall("SELECT * FROM speed_batch_reconciliation WHERE bill_date = %s ORDER BY grid_zone",
                             (bill_date,))

    def batch_days(self) -> list[date]:
        return [r["bill_date"] for r in self.fetchall(
            "SELECT DISTINCT bill_date FROM batch_zone_daily_summary ORDER BY bill_date DESC")]

    # ------------------------------------------------------------------ pipeline / ops
    def batch_runs(self, limit: int) -> list[dict]:
        return self.fetchall("SELECT * FROM batch_runs ORDER BY finished_at DESC NULLS LAST LIMIT %s", (limit,))

    def freshness(self) -> dict:
        return self.fetchone("""
            SELECT (SELECT extract(epoch FROM now() - max(updated_at)) FROM rt_zone_metrics) AS speed_age_s,
                   (SELECT max(window_start) FROM rt_zone_metrics) AS speed_latest_window,
                   (SELECT max(bill_date) FROM batch_runs WHERE status = 'SUCCESS') AS batch_latest_day,
                   (SELECT count(*) FROM grid_alerts WHERE created_at > now() - interval '5 minutes') AS recent_alerts,
                   (SELECT extract(epoch FROM now() - max(speed_processed_at)) FROM pipeline_event_trace)
                        AS trace_age_s""") or {}

    def trace(self, event_id: str) -> dict | None:
        return self.fetchone("SELECT * FROM pipeline_event_trace WHERE event_id = %s", (event_id,))

    def recent_traces(self, limit: int) -> list[dict]:
        return self.fetchall("SELECT * FROM pipeline_event_trace ORDER BY speed_processed_at DESC LIMIT %s", (limit,))

    def latency_summary(self) -> dict:
        return self.fetchone("""
            SELECT count(*) AS samples,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY e2e_latency_ms) AS p50_ms,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e_latency_ms) AS p95_ms,
                   max(e2e_latency_ms) AS max_ms
            FROM pipeline_event_trace WHERE speed_processed_at > now() - interval '10 minutes'
              AND NOT is_late""") or {}

    def quality_summary(self) -> dict:
        return self.fetchone("""
            SELECT COALESCE(sum(valid_count), 0) AS valid, COALESCE(sum(invalid_count), 0) AS invalid,
                   COALESCE(sum(late_count), 0) AS late
            FROM stream_quality_stats WHERE processed_at > now() - interval '10 minutes'""") or {}

    def insert_ops_alert(self, row: tuple) -> None:
        self.execute("""INSERT INTO ops_alerts (alertname, status, severity, summary, labels, starts_at, ends_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""", row)

    def ops_alerts(self, limit: int) -> list[dict]:
        return self.fetchall("SELECT * FROM ops_alerts ORDER BY received_at DESC LIMIT %s", (limit,))
