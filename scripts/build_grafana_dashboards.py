"""Generates the provisioned Grafana dashboards (infra/grafana/dashboards/*.json).

Dashboards are kept as code: edit this file and re-run
    python scripts/build_grafana_dashboards.py
rather than hand-editing the large JSON files.
"""
from __future__ import annotations

import json
import os

PG = {"type": "grafana-postgresql-datasource", "uid": "smartgrid-pg"}
PROM = {"type": "prometheus", "uid": "smartgrid-prom"}
OUT = os.path.join(os.path.dirname(__file__), "..", "infra", "grafana", "dashboards")

LATEST_ZONES = """(SELECT DISTINCT ON (grid_zone) * FROM rt_zone_metrics WHERE intervals_observed >= 2
                   ORDER BY grid_zone, window_start DESC) z"""


class Board:
    def __init__(self, uid, title, description, time_from="now-30m", refresh="10s", tags=()):
        self.uid, self.title, self.description = uid, title, description
        self.time_from, self.refresh, self.tags = time_from, refresh, list(tags)
        self.panels, self.templating, self._id, self._y = [], [], 1, 0

    def row(self, title):
        self.panels.append({"type": "row", "title": title, "id": self._id, "collapsed": False,
                            "gridPos": {"h": 1, "w": 24, "x": 0, "y": self._y}, "panels": []})
        self._id += 1
        self._y += 1

    def add(self, panels_with_widths, height):
        x = 0
        for p, w in panels_with_widths:
            p["id"] = self._id
            p["gridPos"] = {"h": height, "w": w, "x": x, "y": self._y}
            self._id += 1
            x += w
            self.panels.append(p)
        self._y += height

    def to_json(self):
        return {
            "uid": self.uid, "title": self.title, "description": self.description, "tags": self.tags,
            "timezone": "utc", "schemaVersion": 39, "version": 1, "editable": True, "graphTooltip": 1,
            "refresh": self.refresh, "time": {"from": self.time_from, "to": "now"},
            "templating": {"list": self.templating}, "annotations": {"list": []}, "panels": self.panels,
        }


def pg_target(sql, fmt="table", ref="A"):
    return {"refId": ref, "datasource": PG, "rawQuery": True, "editorMode": "code", "format": fmt, "rawSql": sql}


def prom_target(expr, legend="", ref="A", instant=False):
    t = {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend, "range": not instant}
    if instant:
        t["instant"] = True
    return t


def stat(title, targets, unit="short", decimals=None, thresholds=None, color_mode="value", desc=""):
    ds = targets[0]["datasource"]
    defaults = {"unit": unit, "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": thresholds or [{"color": "blue", "value": None}]}}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {"type": "stat", "title": title, "description": desc, "datasource": ds, "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": color_mode, "graphMode": "none", "textMode": "auto", "justifyMode": "auto"}}


def timeseries(title, targets, unit="short", desc="", min_=None, max_=None, thresholds=None, stack=False,
               draw="line"):
    ds = targets[0]["datasource"]
    custom = {"drawStyle": draw, "lineWidth": 2, "fillOpacity": 12 if not stack else 60, "showPoints": "never",
              "spanNulls": True, "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    if thresholds:
        custom["thresholdsStyle"] = {"mode": "dashed"}
    defaults = {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}}
    if min_ is not None:
        defaults["min"] = min_
    if max_ is not None:
        defaults["max"] = max_
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
    return {"type": "timeseries", "title": title, "description": desc, "datasource": ds, "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def table(title, targets, desc="", overrides=None, unit=None):
    ds = targets[0]["datasource"]
    defaults = {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}}
    if unit:
        defaults["unit"] = unit
    return {"type": "table", "title": title, "description": desc, "datasource": ds, "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
            "options": {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}}


def barchart(title, targets, unit="short", desc="", stacking="none", x_field=None, horizontal=False):
    ds = targets[0]["datasource"]
    opts = {"orientation": "horizontal" if horizontal else "auto", "stacking": stacking, "showValue": "auto",
            "barWidth": 0.8, "groupWidth": 0.7, "xTickLabelRotation": 0,
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi"}}
    if x_field:
        opts["xField"] = x_field
    return {"type": "barchart", "title": title, "description": desc, "datasource": ds, "targets": targets,
            "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "palette-classic"},
                                         "custom": {"fillOpacity": 85, "lineWidth": 0}}, "overrides": []},
            "options": opts}


def bargauge(title, targets, unit="percentunit", thresholds=None, desc=""):
    ds = targets[0]["datasource"]
    return {"type": "bargauge", "title": title, "description": desc, "datasource": ds, "targets": targets,
            "fieldConfig": {"defaults": {"unit": unit, "min": 0, "max": 1, "color": {"mode": "thresholds"},
                                         "thresholds": {"mode": "absolute", "steps": thresholds}}, "overrides": []},
            "options": {"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                        "valueMode": "color", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "",
                                                                "values": True}}}


def unit_override(name, unit):
    return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "unit", "value": unit}]}


SHARE_STEPS = [{"color": "red", "value": None}, {"color": "orange", "value": 0.25}, {"color": "green", "value": 0.5}]


# =========================================================================== 1. live operations
def live_ops():
    b = Board("smartgrid-live-ops", "SmartGrid - Live Grid Operations (speed layer)",
              "Real-time grid load and renewable contribution by zone, from the Spark Structured Streaming "
              "speed layer. X axes show REAL time; one real minute = 4.8 simulated hours.",
              tags=["smartgrid", "speed-layer"])
    b.row("Right now")
    b.add([
        (stat("Simulated time", [prom_target("max(smartgrid_sim_time_seconds) * 1000", instant=True)],
              unit="dateTimeAsIso", desc="Current simulated clock (1 day = 5 real minutes)"), 5),
        (stat("Grid load", [pg_target(f"SELECT sum(load_kw) AS load FROM {LATEST_ZONES}")], unit="kwatt",
              decimals=0), 4),
        (stat("Solar generation", [pg_target(f"SELECT sum(solar_kw) AS solar FROM {LATEST_ZONES}")],
              unit="kwatt", decimals=0,
              thresholds=[{"color": "#f2b705", "value": None}]), 4),
        (stat("Renewable share", [pg_target(
            f"SELECT least(sum(solar_kw), sum(load_kw)) / nullif(sum(load_kw), 0) AS share FROM {LATEST_ZONES}")],
              unit="percentunit", decimals=1, thresholds=SHARE_STEPS), 4),
        (stat("Meters reporting", [pg_target(
            f"SELECT sum(active_meters)::float / nullif(sum(expected_meters), 0) AS coverage FROM {LATEST_ZONES}")],
              unit="percentunit", decimals=1,
              thresholds=[{"color": "red", "value": None}, {"color": "orange", "value": 0.9},
                          {"color": "green", "value": 0.97}]), 3),
        (stat("Grid alerts (last 5 min)", [pg_target(
            "SELECT count(*) AS alerts FROM grid_alerts WHERE created_at > now() - interval '5 minutes'")],
              thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}]), 4),
    ], 4)
    b.add([
        (bargauge("Renewable share by zone (current window)", [pg_target(
            f"SELECT grid_zone, renewable_share FROM {LATEST_ZONES} ORDER BY grid_zone")],
                  thresholds=SHARE_STEPS, desc="Share of zone load met by rooftop solar; alert below 25% in daylight"),
         8),
        (table("Zone status (latest window, simulated time)", [pg_target(
            f"""SELECT grid_zone AS "Zone", to_char(window_start, 'YYYY-MM-DD HH24:MI') AS "Window (sim)",
                       round(load_kw::numeric, 1) AS "Load kW", round(solar_kw::numeric, 1) AS "Solar kW",
                       renewable_share AS "Renewable", active_meters || '/' || expected_meters AS "Meters",
                       to_char(updated_at, 'HH24:MI:SS') AS "Updated (real)"
                FROM {LATEST_ZONES} ORDER BY grid_zone""")],
               overrides=[unit_override("Renewable", "percentunit")]), 16),
    ], 7)
    b.row("Trends")
    b.add([
        (timeseries("Load by zone (kW)", [pg_target(
            """SELECT window_start_real AS time, grid_zone AS metric, load_kw AS value FROM rt_zone_metrics
               WHERE $__timeFilter(window_start_real) AND intervals_observed >= 2 ORDER BY 1""", fmt="time_series")],
                    unit="kwatt", stack=True), 12),
        (timeseries("Renewable share by zone", [pg_target(
            """SELECT window_start_real AS time, grid_zone AS metric, renewable_share AS value FROM rt_zone_metrics
               WHERE $__timeFilter(window_start_real) AND intervals_observed >= 2 ORDER BY 1""", fmt="time_series")],
                    unit="percentunit", min_=0, max_=1,
                    thresholds=[{"color": "transparent", "value": None}, {"color": "red", "value": 0.25}]), 12),
    ], 9)
    b.add([
        (timeseries("Solar generation by zone (kW)", [pg_target(
            """SELECT window_start_real AS time, grid_zone AS metric, solar_kw AS value FROM rt_zone_metrics
               WHERE $__timeFilter(window_start_real) AND intervals_observed >= 2 ORDER BY 1""", fmt="time_series")],
                    unit="kwatt"), 12),
        (timeseries("Grid load vs solar (all zones)", [pg_target(
            """SELECT window_start_real AS time, sum(load_kw) AS "Load kW", sum(solar_kw) AS "Solar kW"
               FROM rt_zone_metrics WHERE $__timeFilter(window_start_real) AND intervals_observed >= 2
               GROUP BY 1 ORDER BY 1""", fmt="time_series")], unit="kwatt"), 12),
    ], 8)
    b.row("Alerts")
    b.add([
        (table("Grid alerts raised by the speed layer", [pg_target(
            """SELECT to_char(created_at, 'HH24:MI:SS') AS "Raised (real)",
                      to_char(window_start, 'YYYY-MM-DD HH24:MI') AS "Window (sim)", alert_type AS "Type",
                      grid_zone AS "Zone", severity AS "Severity", message AS "Message"
               FROM grid_alerts ORDER BY created_at DESC LIMIT 50""")]), 14),
        (table("Operational alerts (Prometheus -> Alertmanager -> API)", [pg_target(
            """SELECT to_char(received_at, 'HH24:MI:SS') AS "Received", alertname AS "Alert", status AS "Status",
                      severity AS "Severity", summary AS "Summary"
               FROM ops_alerts ORDER BY received_at DESC LIMIT 50""")]), 10),
    ], 9)
    return b


# =========================================================================== 2. daily billing
def billing():
    b = Board("smartgrid-billing", "SmartGrid - Daily Billing & Solar Contribution (batch layer)",
              "Authoritative daily views recomputed by the Airflow-orchestrated Spark batch job. "
              "Pick a simulated day with the bill_date variable.", time_from="now-6h", refresh="30s",
              tags=["smartgrid", "batch-layer"])
    b.templating.append({
        "name": "bill_date", "label": "Simulated day", "type": "query", "datasource": PG,
        "query": "SELECT DISTINCT bill_date::text FROM batch_zone_daily_summary ORDER BY 1 DESC",
        "definition": "SELECT DISTINCT bill_date::text FROM batch_zone_daily_summary ORDER BY 1 DESC",
        "refresh": 2, "sort": 0, "current": {}, "includeAll": False, "multi": False})
    D = "'$bill_date'::date"
    b.row("Day summary")
    b.add([
        (stat("Total billed (LKR)", [pg_target(f"SELECT sum(total_billed) FROM batch_zone_daily_summary WHERE bill_date = {D}")],
              decimals=0), 4),
        (stat("Households billed", [pg_target(f"SELECT sum(households) FROM batch_zone_daily_summary WHERE bill_date = {D}")]), 4),
        (stat("Average bill (LKR)", [pg_target(f"SELECT avg(amount_due) FROM batch_household_daily_bill WHERE bill_date = {D}")],
              decimals=2), 4),
        (stat("Renewable share", [pg_target(
            f"""SELECT least(sum(solar_kwh), sum(consumption_kwh)) / nullif(sum(consumption_kwh), 0)
                FROM batch_zone_daily_summary WHERE bill_date = {D}""")], unit="percentunit", decimals=1,
              thresholds=SHARE_STEPS), 4),
        (stat("Feed-in credits (LKR)", [pg_target(
            f"SELECT sum(total_feed_in_credit) FROM batch_zone_daily_summary WHERE bill_date = {D}")], decimals=0,
              thresholds=[{"color": "#f2b705", "value": None}]), 4),
        (stat("Late readings recovered", [pg_target(
            f"SELECT late_records FROM batch_runs WHERE bill_date = {D} AND status = 'SUCCESS' ORDER BY finished_at DESC LIMIT 1")],
              desc="Readings the speed layer dropped (after watermark) that the batch layer included",
              thresholds=[{"color": "purple", "value": None}]), 4),
    ], 4)
    b.add([
        (barchart("Consumption vs solar by zone (kWh)", [pg_target(
            f"""SELECT grid_zone, consumption_kwh AS "Consumption kWh", solar_kwh AS "Solar kWh"
                FROM batch_zone_daily_summary WHERE bill_date = {D} ORDER BY grid_zone""")], unit="none"), 8),
        (barchart("Billed amount by zone (LKR)", [pg_target(
            f"""SELECT grid_zone, total_billed AS "Billed LKR" FROM batch_zone_daily_summary
                WHERE bill_date = {D} ORDER BY grid_zone""")]), 8),
        (barchart("Grid-wide load profile by hour (kWh)", [pg_target(
            f"""SELECT lpad(hour_of_day::text, 2, '0') AS hour, sum(consumption_kwh) AS "Consumption",
                       sum(solar_kwh) AS "Solar"
                FROM batch_zone_hourly WHERE bill_date = {D} GROUP BY hour_of_day ORDER BY hour_of_day""")]), 8),
    ], 8)
    b.row("Households")
    b.add([
        (table("Household bills (top 25 by amount due)", [pg_target(
            f"""SELECT household_id AS "Household", grid_zone AS "Zone", tariff_plan AS "Plan",
                       billing_tier AS "Tier", subsidy_flag AS "Subsidy", round(import_kwh::numeric, 2) AS "Import kWh",
                       round(peak_import_kwh::numeric, 2) AS "Peak kWh", round(export_kwh::numeric, 2) AS "Export kWh",
                       round(energy_charge::numeric, 2) AS "Energy", round(block_surcharge::numeric, 2) AS "Block",
                       round(subsidy_discount::numeric, 2) AS "Subsidy LKR", round(feed_in_credit::numeric, 2) AS "Feed-in",
                       round(amount_due::numeric, 2) AS "Amount due", data_completeness AS "Complete"
                FROM batch_household_daily_bill WHERE bill_date = {D} ORDER BY amount_due DESC LIMIT 25""")],
               overrides=[unit_override("Complete", "percentunit")]), 16),
        (table("Zone summary", [pg_target(
            f"""SELECT grid_zone AS "Zone", households AS "HH", round(peak_load_kw::numeric, 1) AS "Peak kW",
                       lpad(peak_hour::text, 2, '0') || ':00' AS "Peak hour", renewable_share AS "Renewable",
                       cloud_cover AS "Cloud", round(avg_bill::numeric, 2) AS "Avg bill"
                FROM batch_zone_daily_summary WHERE bill_date = {D} ORDER BY grid_zone""")],
               overrides=[unit_override("Renewable", "percentunit"), unit_override("Cloud", "percentunit")]), 8),
    ], 10)
    b.row("Accuracy & lineage")
    b.add([
        (barchart("Speed vs batch consumption by zone (kWh)", [pg_target(
            f"""SELECT grid_zone, speed_consumption_kwh AS "Speed layer", batch_consumption_kwh AS "Batch layer"
                FROM speed_batch_reconciliation WHERE bill_date = {D} ORDER BY grid_zone""")],
                  desc="The batch layer recomputes from the complete raw archive; the gap is late data the "
                       "speed layer's watermark dropped"), 8),
        (table("Reconciliation", [pg_target(
            f"""SELECT grid_zone AS "Zone", round(drift_kwh::numeric, 2) AS "Drift kWh", round(drift_pct::numeric, 2) AS "Drift %",
                       late_records AS "Late readings"
                FROM speed_batch_reconciliation WHERE bill_date = {D} ORDER BY grid_zone""")]), 7),
        (table("Batch runs", [pg_target(
            """SELECT bill_date AS "Day", run_type AS "Type", status AS "Status", raw_records AS "Raw",
                      invalid_records AS "Invalid", duplicates_removed AS "Dupes", late_records AS "Late",
                      households_billed AS "HH", tariff_version AS "Tariff v", round(duration_seconds::numeric, 1) AS "Secs",
                      to_char(finished_at, 'HH24:MI:SS') AS "Finished"
               FROM batch_runs ORDER BY finished_at DESC NULLS LAST LIMIT 20""")]), 9),
    ], 9)
    b.add([
        (barchart("Daily trend: total billed (LKR) per simulated day", [pg_target(
            """SELECT bill_date::text AS day, sum(total_billed) AS "Billed LKR"
               FROM batch_zone_daily_summary GROUP BY bill_date ORDER BY bill_date DESC LIMIT 14""")]), 12),
        (barchart("Daily trend: renewable share per zone", [pg_target(
            """SELECT bill_date::text AS day,
                      max(renewable_share) FILTER (WHERE grid_zone = 'COLOMBO') AS "COLOMBO",
                      max(renewable_share) FILTER (WHERE grid_zone = 'GALLE') AS "GALLE",
                      max(renewable_share) FILTER (WHERE grid_zone = 'KANDY') AS "KANDY",
                      max(renewable_share) FILTER (WHERE grid_zone = 'MATARA') AS "MATARA",
                      max(renewable_share) FILTER (WHERE grid_zone = 'JAFFNA') AS "JAFFNA"
               FROM batch_zone_daily_summary GROUP BY bill_date ORDER BY bill_date DESC LIMIT 7""")],
                  unit="percentunit"), 12),
    ], 8)
    return b


# =========================================================================== 3. pipeline health
def pipeline():
    b = Board("smartgrid-pipeline", "SmartGrid - Pipeline Health & Observability",
              "Metrics from every stage: producer, Kafka, Spark speed layer, Airflow batch layer, serving API.",
              time_from="now-30m", refresh="10s", tags=["smartgrid", "observability"])
    b.row("Health at a glance")
    up_steps = [{"color": "red", "value": None}, {"color": "green", "value": 1}]
    b.add([
        (stat("Targets up", [prom_target("sum(up)", instant=True)], thresholds=[{"color": "green", "value": None}]), 3),
        (stat("Firing alerts", [prom_target('count(ALERTS{alertstate="firing"}) or vector(0)', instant=True)],
              thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}]), 3),
        (stat("Producer events/s", [prom_target("sum(rate(smartgrid_producer_events_sent_total[1m]))", instant=True)],
              decimals=1), 3),
        (stat("Stream lag (records)", [prom_target("max(smartgrid_stream_kafka_lag_records)", instant=True)],
              thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 2000},
                          {"color": "red", "value": 5000}]), 3),
        (stat("E2E latency p95", [prom_target(
            "histogram_quantile(0.95, sum(rate(smartgrid_stream_e2e_latency_seconds_bucket[5m])) by (le))",
            instant=True)], unit="s", decimals=1,
              thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 15},
                          {"color": "red", "value": 30}]), 3),
        (stat("Invalid records", [prom_target(
            'sum(rate(smartgrid_stream_records_total{status="invalid"}[5m])) / sum(rate(smartgrid_stream_records_total[5m]))',
            instant=True)], unit="percentunit", decimals=2,
              thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 0.05}]), 3),
        (stat("Last batch run", [prom_target("min(smartgrid_batch_last_status)", instant=True)],
              thresholds=up_steps, desc="1 = success, 0 = failed"), 3),
        (stat("Batch success age", [prom_target("time() - max(smartgrid_batch_last_success_unixtime)", instant=True)],
              unit="s", decimals=0, thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 600}]), 3),
    ], 4)
    b.row("Ingestion (producer + Kafka)")
    b.add([
        (timeseries("Producer events/s by kind", [prom_target(
            "sum by (kind) (rate(smartgrid_producer_events_sent_total[1m]))", "{{kind}}")], unit="short"), 8),
        (timeseries("Kafka messages/s per partition (meter-readings)", [prom_target(
            'sum by (partition) (rate(kafka_topic_partition_current_offset{topic="meter-readings"}[1m]))',
            "p{{partition}}")]), 8),
        (timeseries("Injected faults / s", [prom_target(
            "sum by (fault) (rate(smartgrid_producer_faults_injected_total[2m]))", "{{fault}}")]), 8),
    ], 8)
    b.row("Processing (Spark Structured Streaming)")
    b.add([
        (timeseries("Input vs processed rows/s per query", [
            prom_target("smartgrid_stream_input_rows_per_second", "in {{query}}", "A"),
            prom_target("smartgrid_stream_processed_rows_per_second", "processed {{query}}", "B")]), 8),
        (timeseries("Kafka lag (records behind head)", [prom_target(
            "smartgrid_stream_kafka_lag_records", "{{query}}")]), 8),
        (timeseries("Micro-batch duration", [prom_target(
            "smartgrid_stream_batch_duration_ms", "{{query}}")], unit="ms"), 8),
    ], 8)
    b.add([
        (timeseries("End-to-end latency (producer -> speed view)", [
            prom_target("histogram_quantile(0.5, sum(rate(smartgrid_stream_e2e_latency_seconds_bucket[2m])) by (le))", "p50", "A"),
            prom_target("histogram_quantile(0.95, sum(rate(smartgrid_stream_e2e_latency_seconds_bucket[2m])) by (le))", "p95", "B")],
                    unit="s"), 8),
        (timeseries("Invalid records by reason (/s)", [prom_target(
            "sum by (reason) (rate(smartgrid_stream_invalid_records_total[2m]))", "{{reason}}")]), 8),
        (timeseries("Late records (/s) and state store rows", [
            prom_target("rate(smartgrid_stream_late_records_total[2m])", "late records/s", "A"),
            prom_target("sum(smartgrid_stream_state_rows) / 1000", "state rows (k)", "B")]), 8),
    ], 8)
    b.row("Batch layer & serving")
    b.add([
        (stat("Last batch: raw records", [prom_target("max(smartgrid_batch_last_raw_records)", instant=True)]), 4),
        (stat("Last batch: duplicates removed", [prom_target("max(smartgrid_batch_last_duplicates_removed)", instant=True)]), 4),
        (stat("Last batch: late recovered", [prom_target("max(smartgrid_batch_last_late_records)", instant=True)]), 4),
        (stat("Last batch duration", [prom_target("max(smartgrid_batch_last_duration_seconds)", instant=True)],
              unit="s", decimals=1), 4),
        (stat("Max speed-vs-batch drift", [prom_target("max(smartgrid_batch_last_max_zone_drift_pct)", instant=True)],
              unit="percent", decimals=2), 4),
        (stat("Tariff feed age", [prom_target("time() - max(smartgrid_tariff_last_drop_unixtime)", instant=True)],
              unit="s", decimals=0, thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 600}]), 4),
    ], 4)
    b.add([
        (timeseries("API requests/s by endpoint", [prom_target(
            'sum by (path) (rate(smartgrid_api_requests_total{path!~"/metrics.*"}[1m]))', "{{path}}")]), 8),
        (timeseries("Speed view age (s)", [prom_target("smartgrid_speed_view_age_seconds", "age")], unit="s",
                    thresholds=[{"color": "transparent", "value": None}, {"color": "red", "value": 90}]), 8),
        (table("Firing alerts", [prom_target('ALERTS{alertstate="firing"}', instant=True)]), 8),
    ], 8)
    return b


def main():
    os.makedirs(OUT, exist_ok=True)
    for name, board in [("smartgrid-live-ops", live_ops()), ("smartgrid-billing", billing()),
                        ("smartgrid-pipeline", pipeline())]:
        path = os.path.join(OUT, f"{name}.json")
        with open(path, "w") as fh:
            json.dump(board.to_json(), fh, indent=2)
        print("wrote", os.path.normpath(path))


if __name__ == "__main__":
    main()
