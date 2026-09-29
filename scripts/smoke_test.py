"""End-to-end smoke test against a running stack (run from the host, stdlib only):

    python scripts/smoke_test.py            # checks every layer once
    python scripts/smoke_test.py --wait 900 # keep retrying until the first daily bill exists

Exit code 0 when ingestion, speed layer, batch layer, serving layer and observability all respond.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request

API = "http://localhost:8000"
PROM = "http://localhost:9090"


def get(url: str):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read().decode())


def check(name: str, fn) -> bool:
    try:
        detail = fn()
        print(f"  PASS  {name}: {detail}")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  FAIL  {name}: {exc}")
        return False


def prom(query: str) -> float:
    res = get(f"{PROM}/api/v1/query?query={urllib.parse.quote(query)}")["data"]["result"]
    if not res:
        raise RuntimeError(f"no series for {query}")
    return float(res[0]["value"][1])


PRODUCER_RATE = 'sum(rate(smartgrid_producer_events_sent_total[1m]))'
STREAM_RATE = 'sum(rate(smartgrid_stream_input_rows_total{query="grid_state"}[1m]))'


def run_checks() -> bool:
    checks = [
        ("producer is publishing", lambda: f"{prom(PRODUCER_RATE):.1f} events/s"),
        ("speed layer consuming", lambda: f"{prom(STREAM_RATE):.1f} rows/s"),
        ("real-time zones API", lambda: f"{len(get(API + '/api/v1/realtime/zones')['zones'])} zones"),
        ("grid summary API", lambda: f"{get(API + '/api/v1/realtime/grid')['total_load_kw']} kW"),
        ("batch view exists", lambda: get(API + "/api/v1/billing/days")["days"][0]),
        ("daily report generated", lambda: get(API + "/api/v1/reports")["reports"][0]["date"]),
        ("bill estimate (lambda merge)", lambda: (lambda b: f"{b['days_confirmed']} confirmed + "
                                                  f"{b['days_provisional']} provisional days")(
            get(API + "/api/v1/households/HH-00001/bill-estimate"))),
        ("health endpoint", lambda: get(API + "/health")["status"]),
        ("prometheus alert rules loaded", lambda: f"{sum(len(g['rules']) for g in get(PROM + '/api/v1/rules')['data']['groups'])} rules"),
    ]
    results = [check(n, f) for n, f in checks]
    return all(results)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--wait", type=int, default=0, help="seconds to keep retrying")
    a = p.parse_args()
    deadline = time.time() + a.wait
    while True:
        print(time.strftime("%H:%M:%S"), "smoke test")
        if run_checks():
            print("ALL CHECKS PASSED")
            sys.exit(0)
        if time.time() > deadline:
            sys.exit(1)
        time.sleep(30)


if __name__ == "__main__":
    main()
