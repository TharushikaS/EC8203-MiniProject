"""Daily consolidated billing & solar-contribution report (HTML + CSV).

Answers the business question for simulated day D:
  * What was the grid load and renewable contribution by zone (and by hour of day)?
  * What does each household's bill look like once the day's tariff is applied?
It also shows the data-quality lineage for the day (records read, duplicates removed,
invalid/late readings, tariff version) and the speed-vs-batch reconciliation.

Output: <OUTPUT_DIR>/reports/<D>/daily_report_<D>.html, bills_<D>.csv, zone_summary_<D>.csv
"""
from __future__ import annotations

import base64
import io
import os
from datetime import datetime, timezone

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from jinja2 import Environment, FileSystemLoader, select_autoescape  # noqa: E402

from smartgrid.common import db  # noqa: E402
from smartgrid.common.config import Settings, get_settings  # noqa: E402
from smartgrid.common.logging_utils import get_logger  # noqa: E402

log = get_logger("batch-layer", "serving", name="batch.report")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
ZONE_COLORS = {"COLOMBO": "#2a78d6", "GALLE": "#e07b28", "KANDY": "#3a9a5b", "MATARA": "#8a5cc7", "JAFFNA": "#c7475f"}


def _query(cur, sql: str, params) -> pd.DataFrame:
    cur.execute(sql, params)
    cols = [c.name for c in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def _png(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def _chart_zone_mix(zone: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(7, 3.2))
    x = range(len(zone))
    ax.bar(x, zone["consumption_kwh"], color="#c9d3df", label="Consumption (kWh)")
    ax.bar(x, zone[["solar_kwh", "consumption_kwh"]].min(axis=1), color="#f2b705", label="Met by solar (kWh)")
    ax.set_xticks(list(x), zone["grid_zone"])
    for i, share in enumerate(zone["renewable_share"]):
        ax.text(i, zone["consumption_kwh"].iloc[i], f"{share:.0%}", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("kWh")
    ax.set_title("Consumption and solar contribution by zone")
    ax.legend(frameon=False, fontsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    return _png(fig)


def _chart_hourly(hourly: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(7, 3.2))
    for zone, grp in hourly.groupby("grid_zone"):
        grp = grp.sort_values("hour_of_day")
        ax.plot(grp["hour_of_day"], grp["renewable_share"] * 100, label=zone, color=ZONE_COLORS.get(zone), lw=1.8)
    ax.set_xlabel("Hour of day (simulated)")
    ax.set_ylabel("Renewable share (%)")
    ax.set_xticks(range(0, 24, 2))
    ax.set_ylim(0, 105)
    ax.set_title("Renewable contribution by hour of day")
    ax.legend(frameon=False, fontsize=8, ncol=5, loc="upper center", bbox_to_anchor=(0.5, -0.2))
    ax.spines[["top", "right"]].set_visible(False)
    return _png(fig)


def _chart_bills(bills: pd.DataFrame, currency: str) -> str:
    fig, ax = plt.subplots(figsize=(7, 3.0))
    ax.hist(bills["amount_due"], bins=30, color="#2a78d6", alpha=0.85)
    ax.set_xlabel(f"Daily amount due ({currency})")
    ax.set_ylabel("Households")
    ax.set_title("Distribution of household bills")
    ax.spines[["top", "right"]].set_visible(False)
    return _png(fig)


def generate_daily_report(day: str, settings: Settings | None = None) -> str:
    settings = settings or get_settings()
    with db.transaction(settings) as cur:
        zone = _query(cur, "SELECT * FROM batch_zone_daily_summary WHERE bill_date = %s ORDER BY grid_zone", (day,))
        hourly = _query(cur, "SELECT * FROM batch_zone_hourly WHERE bill_date = %s", (day,))
        bills = _query(cur, """SELECT * FROM batch_household_daily_bill WHERE bill_date = %s
                               ORDER BY amount_due DESC""", (day,))
        recon = _query(cur, "SELECT * FROM speed_batch_reconciliation WHERE bill_date = %s ORDER BY grid_zone", (day,))
        run = _query(cur, """SELECT * FROM batch_runs WHERE bill_date = %s AND status = 'SUCCESS'
                             ORDER BY finished_at DESC LIMIT 1""", (day,))
        alerts = _query(cur, """SELECT alert_type, grid_zone, window_start, severity, metric_value, threshold, message
                                FROM grid_alerts WHERE window_start >= %s::date AND window_start < %s::date + 1
                                ORDER BY window_start""", (day, day))
        tariff = _query(cur, """SELECT tariff_version, source_file, count(*) AS households,
                                       sum(carried_forward::int) AS carried_forward, avg(tariff_rate) AS avg_rate
                                FROM reference_tariffs WHERE bill_date = %s GROUP BY 1, 2""", (day,))
    if bills.empty:
        raise RuntimeError(f"No batch bills for {day}; run the billing job first")

    for df in (zone, hourly, bills, recon):
        for c in df.columns:
            if df[c].dtype == object and c not in ("grid_zone", "household_id", "tariff_plan", "billing_tier",
                                                   "batch_run_id", "alert_type", "severity", "message"):
                try:
                    df[c] = pd.to_numeric(df[c])
                except (ValueError, TypeError):
                    pass

    total_cons = float(zone["consumption_kwh"].sum())
    total_solar = float(zone["solar_kwh"].sum())
    kpis = {
        "households": int(bills["household_id"].nunique()),
        "consumption_kwh": total_cons,
        "solar_kwh": total_solar,
        "renewable_share": min(total_solar, total_cons) / total_cons if total_cons else 0.0,
        "total_billed": float(bills["amount_due"].sum()),
        "avg_bill": float(bills["amount_due"].mean()),
        "feed_in_credit": float(bills["feed_in_credit"].sum()),
        "alerts": len(alerts),
        "low_completeness": int((bills["data_completeness"] < 0.95).sum()),
    }
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=select_autoescape(["html"]))
    html = env.get_template("daily_report.html.j2").render(
        day=day, generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), currency=settings.currency,
        kpis=kpis, zone=zone.to_dict("records"), recon=recon.to_dict("records"),
        run=(run.to_dict("records") or [{}])[0], alerts=alerts.to_dict("records"),
        tariff=(tariff.to_dict("records") or [{}])[0],
        top_bills=bills.head(15).to_dict("records"),
        credit_bills=bills[bills["carried_credit"] > 0].sort_values("carried_credit", ascending=False).head(10)
        .to_dict("records"),
        chart_zone=_chart_zone_mix(zone), chart_hourly=_chart_hourly(hourly),
        chart_bills=_chart_bills(bills, settings.currency),
    )
    out_dir = os.path.join(settings.output_dir, "reports", day)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"daily_report_{day}.html")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html)
    bills.drop(columns=["computed_at"], errors="ignore").to_csv(os.path.join(out_dir, f"bills_{day}.csv"), index=False)
    zone.to_csv(os.path.join(out_dir, f"zone_summary_{day}.csv"), index=False)
    log.info("daily_report_generated", sim_date=day, path=path, households=kpis["households"],
             total_billed=round(kpis["total_billed"], 2), renewable_share=round(kpis["renewable_share"], 4))
    return path
