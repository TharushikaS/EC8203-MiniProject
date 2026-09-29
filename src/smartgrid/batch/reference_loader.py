"""Validate and load the daily tariff + weather drop (batch-source ingestion).

Steps for simulated day D:
  1. Pick the newest version of the tariff file (tariffs_D.csv, tariffs_D.v2.csv, ...).
     Re-issued files are how the billing system publishes corrections; the recompute DAG
     simply reloads the newest version and regenerates the affected bills.
  2. Validate every row (schema, domain values, known household, duplicates). Rejected rows
     go to /data/landing/quarantine/ with a reason column - never silently dropped.
  3. Households missing from today's file carry forward their previous day's tariff
     (flagged ``carried_forward``) so one bad row doesn't leave a customer unbilled.
  4. Write the clean table to the lake (Parquet, partitioned by bill_date) for Spark and to
     PostgreSQL (reference_tariffs / reference_weather) for the serving layer.
"""
from __future__ import annotations

import glob
import json
import os
import re
from datetime import date, datetime, timezone

import pandas as pd

from smartgrid.common import db
from smartgrid.common.billing import PLANS, TIERS
from smartgrid.common.config import Settings, get_settings
from smartgrid.common.households import GRID_ZONES, build_registry
from smartgrid.common.logging_utils import get_logger

log = get_logger("batch-layer", "ingestion", name="batch.reference")

REQUIRED_COLUMNS = ["household_id", "tariff_plan", "tariff_rate", "billing_tier", "subsidy_flag", "feed_in_rate"]
_VERSION_RE = re.compile(r"tariffs_(\d{4}-\d{2}-\d{2})(?:\.v(\d+))?\.csv$")


class ReferenceDataMissing(RuntimeError):
    pass


def find_tariff_file(day: date, settings: Settings) -> tuple[str, int] | None:
    """Return (path, version) of the newest tariff file for ``day`` or None."""
    best: tuple[str, int] | None = None
    for path in glob.glob(os.path.join(settings.landing_dir, "tariffs", f"tariffs_{day.isoformat()}*.csv")):
        m = _VERSION_RE.search(os.path.basename(path))
        if not m:
            continue
        version = int(m.group(2) or 1)
        if best is None or version > best[1]:
            best = (path, version)
    return best


def validate_tariffs(raw: pd.DataFrame, known_households: set[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a raw tariff extract into (clean, rejected-with-reason)."""
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
    if missing_cols:
        raise ValueError(f"tariff file missing columns: {missing_cols}")
    df = raw.copy()
    df["tariff_rate_num"] = pd.to_numeric(df["tariff_rate"], errors="coerce")
    df["feed_in_rate_num"] = pd.to_numeric(df["feed_in_rate"], errors="coerce")
    flag = df["subsidy_flag"].astype(str).str.strip().str.lower()

    reason = pd.Series([None] * len(df), index=df.index, dtype="object")

    def _mark(mask: pd.Series, why: str) -> None:
        reason.loc[mask & reason.isna()] = why

    _mark(df["household_id"].isna() | ~df["household_id"].isin(known_households), "unknown_household")
    _mark(df["tariff_rate_num"].isna(), "missing_rate")
    _mark(df["tariff_rate_num"] <= 0, "non_positive_rate")
    _mark(df["tariff_rate_num"] > 200, "rate_out_of_range")
    _mark(~df["tariff_plan"].isin(list(PLANS)), "unknown_plan")
    _mark(~df["billing_tier"].isin(list(TIERS)), "unknown_tier")
    _mark(~flag.isin(["true", "false", "1", "0", "yes", "no"]), "bad_subsidy_flag")
    _mark(df["feed_in_rate_num"].isna() | (df["feed_in_rate_num"] < 0), "bad_feed_in_rate")

    rejected = raw.loc[reason.notna()].copy()
    rejected["reject_reason"] = reason[reason.notna()]

    good = df.loc[reason.isna()].copy()
    dupes = good.duplicated(subset=["household_id"], keep="last")
    dup_rows = raw.loc[good.index[dupes]].copy()
    dup_rows["reject_reason"] = "duplicate_household"
    rejected = pd.concat([rejected, dup_rows])
    good = good.loc[~dupes]

    clean = pd.DataFrame({
        "household_id": good["household_id"],
        "tariff_plan": good["tariff_plan"],
        "tariff_rate": good["tariff_rate_num"].astype(float),
        "billing_tier": good["billing_tier"],
        "subsidy_flag": flag[good.index].isin(["true", "1", "yes"]),
        "feed_in_rate": good["feed_in_rate_num"].astype(float),
        "carried_forward": False,
    }).reset_index(drop=True)
    return clean, rejected.reset_index(drop=True)


def _previous_tariffs(cur, day: date) -> pd.DataFrame:
    cur.execute(
        """SELECT household_id, tariff_plan, tariff_rate, billing_tier, subsidy_flag, feed_in_rate
           FROM reference_tariffs
           WHERE bill_date = (SELECT max(bill_date) FROM reference_tariffs WHERE bill_date < %s)""",
        (day,),
    )
    cols = ["household_id", "tariff_plan", "tariff_rate", "billing_tier", "subsidy_flag", "feed_in_rate"]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def load_weather(day: date, settings: Settings, cur) -> int:
    path = os.path.join(settings.landing_dir, "weather", f"weather_{day.isoformat()}.json")
    if not os.path.exists(path):
        log.warning("weather_file_missing", sim_date=day.isoformat(), path=path)
        return 0
    with open(path) as fh:
        doc = json.load(fh)
    rows = [(day, zone, float(v["cloud_cover"]), float(v["solar_potential_index"]),
             float(v["forecast_next_day_cloud_cover"])) for zone, v in doc["zones"].items() if zone in GRID_ZONES]
    cur.execute("DELETE FROM reference_weather WHERE bill_date = %s", (day,))
    db.upsert(cur, "reference_weather",
              ["bill_date", "grid_zone", "cloud_cover", "solar_potential_index", "forecast_next_day_cloud_cover"],
              rows, conflict_cols=["bill_date", "grid_zone"])
    out_dir = os.path.join(settings.reference_dir, "weather", f"bill_date={day.isoformat()}")
    os.makedirs(out_dir, exist_ok=True)
    pd.DataFrame(rows, columns=["bill_date", "grid_zone", "cloud_cover", "solar_potential_index",
                                "forecast_next_day_cloud_cover"]).drop(columns=["bill_date"]) \
        .to_parquet(os.path.join(out_dir, "weather.parquet"), index=False)
    return len(rows)


def load_reference_for_day(day: date, settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    found = find_tariff_file(day, settings)
    if not found:
        raise ReferenceDataMissing(f"No tariff file landed for {day}")
    path, version = found
    registry = build_registry(settings.num_households, settings.random_seed)
    known = {h.household_id for h in registry}

    raw = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    clean, rejected = validate_tariffs(raw, known)

    with db.transaction(settings) as cur:
        missing = known - set(clean["household_id"])
        carried = 0
        if missing:
            prev = _previous_tariffs(cur, day)
            prev = prev[prev["household_id"].isin(missing)].copy()
            if not prev.empty:
                prev["carried_forward"] = True
                prev["tariff_rate"] = prev["tariff_rate"].astype(float)
                prev["feed_in_rate"] = prev["feed_in_rate"].astype(float)
                clean = pd.concat([clean, prev], ignore_index=True)
                carried = len(prev)

        cur.execute("DELETE FROM reference_tariffs WHERE bill_date = %s", (day,))
        loaded_at = datetime.now(timezone.utc)
        db.upsert(cur, "reference_tariffs",
                  ["bill_date", "household_id", "tariff_plan", "tariff_rate", "billing_tier", "subsidy_flag",
                   "feed_in_rate", "carried_forward", "tariff_version", "source_file", "loaded_at"],
                  [(day, r.household_id, r.tariff_plan, float(r.tariff_rate), r.billing_tier, bool(r.subsidy_flag),
                    float(r.feed_in_rate), bool(r.carried_forward), version, os.path.basename(path), loaded_at)
                   for r in clean.itertuples()],
                  conflict_cols=["bill_date", "household_id"])
        weather_rows = load_weather(day, settings, cur)

    out_dir = os.path.join(settings.reference_dir, "tariffs", f"bill_date={day.isoformat()}")
    os.makedirs(out_dir, exist_ok=True)
    for old in glob.glob(os.path.join(out_dir, "*.parquet")):
        os.remove(old)
    clean.assign(tariff_version=version).to_parquet(os.path.join(out_dir, "tariffs.parquet"), index=False)

    if not rejected.empty:
        os.makedirs(settings.quarantine_dir, exist_ok=True)
        rejected.to_csv(os.path.join(settings.quarantine_dir, f"tariffs_{day.isoformat()}_v{version}_rejected.csv"),
                        index=False)

    summary = {
        "sim_date": day.isoformat(), "tariff_file": os.path.basename(path), "tariff_version": version,
        "rows_in_file": len(raw), "rows_valid": int((~clean["carried_forward"]).sum()),
        "rows_rejected": len(rejected), "carried_forward": carried,
        "households_without_tariff": len(known - set(clean["household_id"])), "weather_zones": weather_rows,
        "reject_reasons": rejected["reject_reason"].value_counts().to_dict() if not rejected.empty else {},
    }
    log.info("reference_data_loaded", **summary)
    return summary

