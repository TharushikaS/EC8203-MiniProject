"""Physical models used by the simulators: household load profile, solar output, weather.

The weather is a deterministic function of (date, zone, seed). The daily batch source
publishes it as a weather/forecast file, and the meter simulator uses the same function
to shape solar generation, so the two sources are consistent without talking to each other.
"""
from __future__ import annotations

import hashlib
import math
import random
from datetime import date

from smartgrid.common.households import Household

# Hour-of-day load multipliers (0..1) per occupancy profile. Sri Lankan residential
# demand has a small morning peak and a pronounced evening peak (18:30 - 22:30).
_PROFILES = {
    "family":  [0.10, 0.08, 0.07, 0.07, 0.08, 0.20, 0.45, 0.55, 0.35, 0.25, 0.25, 0.30,
                0.35, 0.30, 0.28, 0.30, 0.40, 0.60, 0.90, 1.00, 0.95, 0.80, 0.50, 0.25],
    "working": [0.08, 0.07, 0.06, 0.06, 0.07, 0.25, 0.55, 0.45, 0.15, 0.10, 0.10, 0.10,
                0.12, 0.10, 0.10, 0.12, 0.20, 0.50, 0.85, 1.00, 1.00, 0.85, 0.55, 0.20],
    "retired": [0.10, 0.08, 0.08, 0.08, 0.10, 0.25, 0.45, 0.55, 0.55, 0.50, 0.50, 0.55,
                0.55, 0.50, 0.45, 0.45, 0.50, 0.65, 0.85, 0.90, 0.80, 0.60, 0.35, 0.15],
}


def _unit_hash(*parts: object) -> float:
    """Deterministic pseudo-random number in [0, 1) from arbitrary parts."""
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()
    return int(digest[:12], 16) / float(1 << 48)


def daily_cloud_cover(day: date, zone: str, seed: int) -> float:
    """Average cloud cover (0 = clear, 1 = overcast) for a zone on a given day.

    Roughly one day in six is an overcast "monsoon" day for a zone, which drives
    renewable contribution down and should trigger the low-renewable alert.
    """
    u = _unit_hash("cloud", day.isoformat(), zone, seed)
    if _unit_hash("storm", day.isoformat(), zone, seed) < 0.17:
        return round(0.88 + 0.12 * u, 3)
    return round(0.05 + 0.55 * u, 3)


def hourly_cloud_cover(day: date, hour: int, zone: str, seed: int) -> float:
    base = daily_cloud_cover(day, zone, seed)
    wobble = (_unit_hash("hcloud", day.isoformat(), hour, zone, seed) - 0.5) * 0.2
    return min(1.0, max(0.0, base + wobble))


def clear_sky_factor(hour_float: float) -> float:
    """Normalised clear-sky irradiance: 0 at night, 1 at solar noon (~12:15 in Sri Lanka)."""
    if hour_float <= 6.0 or hour_float >= 18.5:
        return 0.0
    return max(0.0, math.sin(math.pi * (hour_float - 6.0) / 12.5))


def interval_consumption_kwh(h: Household, hour_float: float, interval_minutes: int,
                             rng: random.Random) -> float:
    profile = _PROFILES[h.occupancy]
    lo = profile[int(hour_float) % 24]
    hi = profile[(int(hour_float) + 1) % 24]
    frac = hour_float - int(hour_float)
    shape = lo + (hi - lo) * frac
    kw = h.base_load_kw + h.peak_load_kw * shape
    kw *= rng.lognormvariate(0.0, 0.18)          # appliance noise
    return round(kw * interval_minutes / 60.0, 4)


def interval_solar_kwh(h: Household, day: date, hour_float: float, interval_minutes: int,
                       seed: int, rng: random.Random) -> float:
    if not h.has_solar:
        return 0.0
    cloud = hourly_cloud_cover(day, int(hour_float), h.grid_zone, seed)
    kw = h.solar_kwp * 0.82 * clear_sky_factor(hour_float) * (1.0 - 0.88 * cloud)
    kw *= rng.uniform(0.9, 1.05)
    return round(max(0.0, kw) * interval_minutes / 60.0, 4)
