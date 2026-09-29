"""Query-time merge of batch and speed views - the serving half of the Lambda architecture.

For a household's month-to-date bill:
  * days that the batch layer has already billed use the BATCH bill (authoritative),
  * days that are not yet billed (today, and yesterday until the nightly job runs) are
    priced from the SPEED layer's running totals with the household's latest known tariff,
    using the very same ``compute_bill`` function the batch job uses.
Each line is labelled with its ``source`` so a consumer knows what is final and what is provisional.
"""
from __future__ import annotations

from datetime import date

from smartgrid.common.billing import compute_bill


def merge_month_to_date(batch_rows: list[dict], speed_rows: list[dict], tariff: dict | None,
                        readings_per_day: int) -> dict:
    lines = []
    billed_days = set()
    for b in batch_rows:
        billed_days.add(b["bill_date"])
        lines.append({
            "date": b["bill_date"].isoformat(), "source": "batch", "final": True,
            "consumption_kwh": round(b["consumption_kwh"], 3), "import_kwh": round(b["import_kwh"], 3),
            "export_kwh": round(b["export_kwh"], 3), "amount_due": b["amount_due"],
            "carried_credit": b["carried_credit"], "tariff_rate": b["tariff_rate"],
            "data_completeness": b["data_completeness"],
        })
    for s in speed_rows:
        day: date = s["sim_date"]
        if day in billed_days:
            continue
        line = {
            "date": day.isoformat(), "source": "speed", "final": False,
            "consumption_kwh": round(s["consumption_kwh"], 3), "import_kwh": round(s["import_kwh"], 3),
            "export_kwh": round(s["export_kwh"], 3),
            "data_completeness": round(s["reading_count"] / readings_per_day, 4),
        }
        if tariff:
            bill = compute_bill(import_kwh=s["import_kwh"], peak_import_kwh=s["peak_import_kwh"],
                                export_kwh=s["export_kwh"], tariff_rate=tariff["tariff_rate"],
                                tariff_plan=tariff["tariff_plan"], billing_tier=tariff["billing_tier"],
                                subsidy_flag=tariff["subsidy_flag"], feed_in_rate=tariff["feed_in_rate"])
            line.update(amount_due=bill.amount_due, carried_credit=bill.carried_credit,
                        tariff_rate=tariff["tariff_rate"], tariff_as_of=tariff["bill_date"].isoformat())
        else:
            line.update(amount_due=None, carried_credit=None, tariff_rate=None, tariff_as_of=None)
        lines.append(line)
    lines.sort(key=lambda x: x["date"])
    confirmed = round(sum(x["amount_due"] for x in lines if x["final"]), 2)
    provisional = round(sum(x["amount_due"] or 0 for x in lines if not x["final"]), 2)
    return {
        "days_confirmed": sum(1 for x in lines if x["final"]),
        "days_provisional": sum(1 for x in lines if not x["final"]),
        "confirmed_amount": confirmed,
        "provisional_amount": provisional,
        "estimated_total": round(confirmed + provisional, 2),
        "lines": lines,
    }
