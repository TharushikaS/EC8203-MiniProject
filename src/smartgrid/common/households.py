"""Deterministic household / meter registry.

Both simulators (the meter stream and the billing-system daily drop) build the same
registry from the same seed, which mimics a real utility where the metering system
and the billing system share a customer master (household_id is the join key).
"""
from __future__ import annotations

import random
from dataclasses import asdict, dataclass

GRID_ZONES: tuple[str, ...] = ("COLOMBO", "GALLE", "KANDY", "MATARA", "JAFFNA")

# Relative share of households per zone (Colombo is the densest).
_ZONE_WEIGHTS = (0.32, 0.20, 0.18, 0.16, 0.14)

TARIFF_PLANS: tuple[str, ...] = ("FLAT", "TOU", "GREEN")
BILLING_TIERS: tuple[str, ...] = ("LIFELINE", "STANDARD", "HIGH_USAGE")
OCCUPANCY_PROFILES: tuple[str, ...] = ("family", "working", "retired")


@dataclass(frozen=True)
class Household:
    household_id: str
    meter_id: str
    grid_zone: str
    occupancy: str
    base_load_kw: float        # always-on load (fridge, standby)
    peak_load_kw: float        # additional load at the household's busiest hour
    has_solar: bool
    solar_kwp: float           # rooftop PV capacity, 0 if no solar
    tariff_plan: str
    billing_tier: str
    subsidy_flag: bool

    def to_dict(self) -> dict:
        return asdict(self)


def build_registry(num_households: int, seed: int) -> list[Household]:
    rng = random.Random(seed)
    registry: list[Household] = []
    for i in range(1, num_households + 1):
        zone = rng.choices(GRID_ZONES, weights=_ZONE_WEIGHTS, k=1)[0]
        occupancy = rng.choice(OCCUPANCY_PROFILES)
        base = round(rng.uniform(0.12, 0.35), 3)
        peak = round(rng.uniform(0.6, 2.4), 3)
        # Solar penetration differs by zone (sunnier south / newer suburbs).
        solar_p = {"COLOMBO": 0.30, "GALLE": 0.40, "KANDY": 0.22, "MATARA": 0.42, "JAFFNA": 0.35}[zone]
        has_solar = rng.random() < solar_p
        kwp = round(rng.uniform(2.0, 6.0), 1) if has_solar else 0.0
        plan = rng.choices(TARIFF_PLANS, weights=(0.55, 0.30, 0.15), k=1)[0]
        if has_solar and rng.random() < 0.5:
            plan = "GREEN"
        expected_daily_kwh = (base * 24) + peak * 5
        if expected_daily_kwh < 9:
            tier = "LIFELINE"
        elif expected_daily_kwh < 13:
            tier = "STANDARD"
        else:
            tier = "HIGH_USAGE"
        registry.append(
            Household(
                household_id=f"HH-{i:05d}",
                meter_id=f"MTR-{i:05d}",
                grid_zone=zone,
                occupancy=occupancy,
                base_load_kw=base,
                peak_load_kw=peak,
                has_solar=has_solar,
                solar_kwp=kwp,
                tariff_plan=plan,
                billing_tier=tier,
                subsidy_flag=(tier == "LIFELINE" and rng.random() < 0.6) or rng.random() < 0.05,
            )
        )
    return registry


def registry_by_id(registry: list[Household]) -> dict[str, Household]:
    return {h.household_id: h for h in registry}
