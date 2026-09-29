from datetime import date

import pandas as pd

from smartgrid.batch.reference_loader import find_tariff_file, validate_tariffs
from smartgrid.serving.lambda_merge import merge_month_to_date

KNOWN = {"HH-00001", "HH-00002", "HH-00003"}


def _row(hh, rate="30.0", tier="STANDARD", plan="FLAT", flag="false", feed="22"):
    return {"household_id": hh, "tariff_plan": plan, "tariff_rate": rate, "billing_tier": tier,
            "subsidy_flag": flag, "feed_in_rate": feed}


def test_validation_rejects_bad_rows_with_reasons_and_dedupes():
    raw = pd.DataFrame([
        _row("HH-00001"),
        _row("HH-00002", rate="-5"),
        _row("HH-00002"),
        _row("HH-00003", tier="PLATINUM"),
        _row("HH-00003", rate=""),
        _row("HH-99999"),
        _row("HH-00001", rate="31.0"),          # duplicate: last one wins
    ])
    clean, rejected = validate_tariffs(raw, KNOWN)
    assert set(clean["household_id"]) == {"HH-00001", "HH-00002"}
    assert clean.set_index("household_id").loc["HH-00001", "tariff_rate"] == 31.0
    assert set(rejected["reject_reason"]) == {"non_positive_rate", "unknown_tier", "missing_rate",
                                              "unknown_household", "duplicate_household"}


def test_newest_tariff_version_wins(settings, tmp_path):
    d = f"{settings.landing_dir}/tariffs"
    import os
    os.makedirs(d, exist_ok=True)
    for name in ("tariffs_2026-03-02.csv", "tariffs_2026-03-02.v3.csv", "tariffs_2026-03-02.v2.csv"):
        open(os.path.join(d, name), "w").write("x")
    path, version = find_tariff_file(date(2026, 3, 2), settings)
    assert version == 3 and path.endswith(".v3.csv")


def test_merge_prefers_batch_and_prices_unbilled_days_from_speed_view():
    batch = [{"bill_date": date(2026, 3, 1), "consumption_kwh": 10.0, "import_kwh": 9.0, "export_kwh": 0.0,
              "amount_due": 300.0, "carried_credit": 0.0, "tariff_rate": 30.0, "data_completeness": 1.0}]
    speed = [
        {"sim_date": date(2026, 3, 1), "consumption_kwh": 8.0, "import_kwh": 7.0, "export_kwh": 0.0,
         "peak_import_kwh": 1.0, "reading_count": 80},                     # already billed -> ignored
        {"sim_date": date(2026, 3, 2), "consumption_kwh": 4.0, "import_kwh": 4.0, "export_kwh": 0.0,
         "peak_import_kwh": 0.0, "reading_count": 48},
    ]
    tariff = {"bill_date": date(2026, 3, 1), "tariff_rate": 30.0, "tariff_plan": "FLAT",
              "billing_tier": "STANDARD", "subsidy_flag": False, "feed_in_rate": 22.0}
    out = merge_month_to_date(batch, speed, tariff, readings_per_day=96)
    assert out["days_confirmed"] == 1 and out["days_provisional"] == 1
    assert out["confirmed_amount"] == 300.0
    assert out["provisional_amount"] == 4.0 * 30.0 + 25.0
    assert [line["source"] for line in out["lines"]] == ["batch", "speed"]
