"""Billing rules - the single source of truth for how a household's day is priced.

The same ``compute_bill`` function is used by:
  * the BATCH layer (Spark ``mapInPandas`` in ``batch/batch_billing.py``) to produce the
    authoritative daily bills, and
  * the SERVING layer to price the speed layer's provisional "today so far" figures.

Keeping one implementation guarantees that the provisional (speed) and confirmed
(batch) numbers only differ because of data completeness, never because of logic drift.

Tariff model (loosely modelled on Sri Lankan domestic block tariffs):
  energy charge   = rate * (off-peak import * off-peak multiplier + peak import * peak multiplier)
  block surcharge = import above the tier's daily block limit is charged at a higher rate
  fixed charge    = per-tier daily service charge
  subsidy         = 25 % of (energy + block) for subsidised households, capped per day
  feed-in credit  = exported solar kWh * feed-in rate
  net             = fixed + energy + block - subsidy - credit ; amount_due = max(net, 0)
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

PEAK_HOURS = frozenset(range(18, 22))   # 18:00-21:59 simulated local time
SUBSIDY_RATE = 0.25
SUBSIDY_DAILY_CAP = 150.0


@dataclass(frozen=True)
class TierRule:
    block_limit_kwh: float
    above_block_multiplier: float
    fixed_daily_charge: float


@dataclass(frozen=True)
class PlanRule:
    peak_multiplier: float
    offpeak_multiplier: float


TIERS: dict[str, TierRule] = {
    "LIFELINE":   TierRule(block_limit_kwh=4.0, above_block_multiplier=1.60, fixed_daily_charge=10.0),
    "STANDARD":   TierRule(block_limit_kwh=8.0, above_block_multiplier=1.35, fixed_daily_charge=25.0),
    "HIGH_USAGE": TierRule(block_limit_kwh=8.0, above_block_multiplier=1.60, fixed_daily_charge=60.0),
}

PLANS: dict[str, PlanRule] = {
    "FLAT":  PlanRule(peak_multiplier=1.00, offpeak_multiplier=1.00),
    "TOU":   PlanRule(peak_multiplier=1.80, offpeak_multiplier=0.80),
    "GREEN": PlanRule(peak_multiplier=1.20, offpeak_multiplier=1.00),
}


@dataclass(frozen=True)
class BillBreakdown:
    energy_charge: float
    block_surcharge: float
    fixed_charge: float
    subsidy_discount: float
    feed_in_credit: float
    net_amount: float
    amount_due: float
    carried_credit: float

    def to_dict(self) -> dict:
        return asdict(self)


def compute_bill(*, import_kwh: float, peak_import_kwh: float, export_kwh: float,
                 tariff_rate: float, tariff_plan: str, billing_tier: str,
                 subsidy_flag: bool, feed_in_rate: float) -> BillBreakdown:
    """Price one household-day. All kWh inputs are daily totals."""
    tier = TIERS[billing_tier]
    plan = PLANS[tariff_plan]
    import_kwh = max(0.0, float(import_kwh))
    peak_import_kwh = min(max(0.0, float(peak_import_kwh)), import_kwh)
    export_kwh = max(0.0, float(export_kwh))

    offpeak = import_kwh - peak_import_kwh
    energy = tariff_rate * (offpeak * plan.offpeak_multiplier + peak_import_kwh * plan.peak_multiplier)
    block = max(0.0, import_kwh - tier.block_limit_kwh) * tariff_rate * (tier.above_block_multiplier - 1.0)
    fixed = tier.fixed_daily_charge
    subsidy = min((energy + block) * SUBSIDY_RATE, SUBSIDY_DAILY_CAP) if subsidy_flag else 0.0
    credit = export_kwh * feed_in_rate
    net = fixed + energy + block - subsidy - credit
    return BillBreakdown(
        energy_charge=round(energy, 2),
        block_surcharge=round(block, 2),
        fixed_charge=round(fixed, 2),
        subsidy_discount=round(subsidy, 2),
        feed_in_credit=round(credit, 2),
        net_amount=round(net, 2),
        amount_due=round(max(net, 0.0), 2),
        carried_credit=round(max(-net, 0.0), 2),
    )
