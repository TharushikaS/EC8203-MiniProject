"""Simulated (compressed) clock shared by every service.

One simulated day lasts ``SIM_DAY_SECONDS`` real seconds (default 300 s = 5 min,
i.e. time runs 288x faster). The mapping is anchored by a single JSON file on the
shared volume (``/data/state/sim_clock.json``). The first service that starts creates
it atomically; every other service (and every restart) reads the same anchor, so the
simulators, the Spark jobs, Airflow and the API all agree on "what simulated time is it".

    sim_time = sim_start + (real_time - anchor_real) * speed
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

CLOCK_FILE = "sim_clock.json"


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class SimClock:
    anchor_real: float          # epoch seconds (real) at which sim_start happened
    sim_start: datetime         # tz-aware UTC
    sim_day_seconds: float

    @property
    def speed(self) -> float:
        """Simulated seconds that elapse per real second."""
        return 86400.0 / self.sim_day_seconds

    @property
    def sim_start_epoch(self) -> float:
        return self.sim_start.timestamp()

    def now(self, real_epoch: float | None = None) -> datetime:
        real_epoch = time.time() if real_epoch is None else real_epoch
        return self.sim_start + timedelta(seconds=(real_epoch - self.anchor_real) * self.speed)

    def today(self) -> date:
        return self.now().date()

    def to_real_epoch(self, sim_dt: datetime) -> float:
        """Real epoch second at which a given simulated instant happened."""
        return self.anchor_real + (sim_dt.timestamp() - self.sim_start_epoch) / self.speed

    def sim_to_real_seconds(self, sim_seconds: float) -> float:
        return sim_seconds / self.speed

    def as_dict(self) -> dict:
        return {
            "anchor_real": self.anchor_real,
            "sim_start": self.sim_start.isoformat(),
            "sim_day_seconds": self.sim_day_seconds,
        }


def load_or_create_clock(state_dir: str, sim_start: str, sim_day_seconds: float) -> SimClock:
    """Return the shared clock, creating the anchor file atomically if it is absent."""
    os.makedirs(state_dir, exist_ok=True)
    path = os.path.join(state_dir, CLOCK_FILE)
    if not os.path.exists(path):
        clock = SimClock(time.time(), _parse_iso(sim_start), float(sim_day_seconds))
        try:
            # O_EXCL guarantees only one service wins the race to create the anchor.
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            with os.fdopen(fd, "w") as fh:
                json.dump(clock.as_dict(), fh)
            return clock
        except FileExistsError:
            pass
    # Another process may be mid-write; retry briefly until the JSON is complete.
    for _ in range(50):
        try:
            with open(path) as fh:
                raw = json.load(fh)
            return SimClock(float(raw["anchor_real"]), _parse_iso(raw["sim_start"]),
                            float(raw["sim_day_seconds"]))
        except (json.JSONDecodeError, KeyError):
            time.sleep(0.1)
    raise RuntimeError(f"Could not read simulated clock file {path}")


def get_clock(settings=None, wait: bool = False, timeout_s: float = 120.0) -> SimClock:
    """Convenience accessor. With ``wait=True`` only readers block until a writer exists."""
    from smartgrid.common.config import get_settings

    settings = settings or get_settings()
    path = os.path.join(settings.state_dir, CLOCK_FILE)
    if wait:
        deadline = time.time() + timeout_s
        while not os.path.exists(path) and time.time() < deadline:
            time.sleep(1)
    return load_or_create_clock(settings.state_dir, settings.sim_start, settings.sim_day_seconds)
