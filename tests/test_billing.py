import pytest

from smartgrid.common.billing import SUBSIDY_DAILY_CAP, TIERS, compute_bill


def bill(**overrides):
    args = dict(import_kwh=6.0, peak_import_kwh=2.0, export_kwh=0.0, tariff_rate=30.0,
                tariff_plan="FLAT", billing_tier="STANDARD", subsidy_flag=False, feed_in_rate=22.0)
    args.update(overrides)
    return compute_bill(**args)


def test_flat_plan_under_block_limit_is_rate_times_import_plus_fixed():
    b = bill()
    assert b.energy_charge == pytest.approx(180.0)
    assert b.block_surcharge == 0
    assert b.fixed_charge == TIERS["STANDARD"].fixed_daily_charge
    assert b.amount_due == pytest.approx(180.0 + 25.0)


def test_block_surcharge_applies_only_above_limit():
    b = bill(import_kwh=10.0, peak_import_kwh=0.0)
    # 2 kWh above the 8 kWh STANDARD block at +35 %
    assert b.block_surcharge == pytest.approx(2 * 30.0 * 0.35)


def test_time_of_use_prices_peak_higher_than_offpeak():
    peak_heavy = bill(tariff_plan="TOU", peak_import_kwh=5.0)
    offpeak_heavy = bill(tariff_plan="TOU", peak_import_kwh=0.0)
    assert peak_heavy.energy_charge > offpeak_heavy.energy_charge


def test_subsidy_is_capped():
    b = bill(import_kwh=200.0, peak_import_kwh=0.0, subsidy_flag=True)
    assert b.subsidy_discount == SUBSIDY_DAILY_CAP


def test_solar_export_produces_credit_and_never_negative_amount_due():
    b = bill(import_kwh=0.5, peak_import_kwh=0.0, export_kwh=20.0, tariff_plan="GREEN", feed_in_rate=27.0)
    assert b.amount_due == 0
    assert b.carried_credit > 0
    assert b.net_amount == pytest.approx(-b.carried_credit)


def test_peak_import_cannot_exceed_total_import():
    b = bill(import_kwh=1.0, peak_import_kwh=5.0)
    assert b.energy_charge == pytest.approx(30.0)   # clamped to 1 kWh of peak on FLAT


def test_unknown_tier_is_rejected():
    with pytest.raises(KeyError):
        bill(billing_tier="PLATINUM")
