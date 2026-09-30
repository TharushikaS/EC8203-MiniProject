"""Demo helper: the billing system re-issues a CORRECTED tariff file for a past day.

    docker compose exec airflow-scheduler python /opt/smartgrid/scripts/issue_tariff_correction.py 2026-03-02 --pct 10 --recompute

Writes tariffs_<date>.v<N+1>.csv with every tariff_rate changed by --pct percent. With
--recompute it also triggers the smartgrid_recompute DAG for that date (the JSON conf is built
here, so the command works the same in PowerShell 5, PowerShell 7, cmd and bash). The batch
layer reloads the newest tariff version and regenerates the bills and report from the
immutable raw archive (the Lambda "recompute" story).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import date

sys.path.insert(0, "/opt/smartgrid/src")

from smartgrid.batch.reference_loader import find_tariff_file  # noqa: E402
from smartgrid.common.config import get_settings  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("day", type=date.fromisoformat)
    p.add_argument("--pct", type=float, default=10.0, help="percentage change applied to every tariff_rate")
    p.add_argument("--recompute", action="store_true", help="also trigger the smartgrid_recompute DAG")
    a = p.parse_args()
    settings = get_settings()
    found = find_tariff_file(a.day, settings)
    if not found:
        raise SystemExit(f"No tariff file exists for {a.day}")
    src, version = found
    dst = os.path.join(os.path.dirname(src), f"tariffs_{a.day.isoformat()}.v{version + 1}.csv")
    with open(src, newline="") as fh:
        rows = list(csv.DictReader(fh))
    for r in rows:
        try:
            r["tariff_rate"] = f"{float(r['tariff_rate']) * (1 + a.pct / 100):.3f}"
        except ValueError:
            pass            # keep the deliberately broken rows broken
    tmp = dst + ".tmp"
    with open(tmp, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, dst)
    print(f"Issued correction {os.path.basename(dst)} ({a.pct:+.1f}% rates) replacing v{version}")
    conf = {"start_date": a.day.isoformat(), "end_date": a.day.isoformat(),
            "reason": f"tariff correction v{version + 1}"}
    if a.recompute:
        subprocess.run(["airflow", "dags", "trigger", "smartgrid_recompute", "--conf", json.dumps(conf)], check=True)
        print("Triggered smartgrid_recompute - watch it in Airflow (http://localhost:8080)")
    else:
        print("Re-run with --recompute to trigger the smartgrid_recompute DAG for this day.")

if __name__ == "__main__":
    main()
