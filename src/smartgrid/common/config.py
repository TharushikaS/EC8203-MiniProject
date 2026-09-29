"""Central configuration.

Every tunable of the platform is read from environment variables here and nowhere
else, so that `docker-compose.yml` / `.env` is the single place to change behaviour.
Defaults are chosen so the stack runs out of the box on a laptop.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _str(name: str, default: str) -> str:
    return os.getenv(name, default)


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Settings:
    # --- Kafka ---------------------------------------------------------------
    kafka_bootstrap: str
    topic_meter_readings: str
    topic_dlq: str
    topic_alerts: str

    # --- Simulated clock -------------------------------------------------------
    sim_start: str             # ISO date-time (UTC) at which simulated time begins
    sim_day_seconds: float     # real seconds that make up one simulated day
    interval_minutes: int      # smart-meter reporting interval (simulated minutes)

    # --- Simulation population & fault injection -------------------------------
    num_households: int
    random_seed: int
    duplicate_rate: float      # probability a reading is re-sent (at-least-once retry)
    invalid_rate: float        # probability a reading is corrupted
    outage_rate: float         # per meter per interval probability of going offline
    outage_max_hours: int      # max simulated hours a meter buffers before flushing
    tariff_bad_row_rate: float
    tariff_late_probability: float

    # --- Storage paths (shared Docker volume) ----------------------------------
    data_dir: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    # --- Processing -------------------------------------------------------------
    watermark_sim_minutes: int
    zone_window_sim_minutes: int
    stream_trigger_seconds: int
    archive_trigger_seconds: int
    low_renewable_threshold: float
    zone_capacity_kw_per_household: float
    trace_sample_pct: int
    batch_grace_sim_minutes: int
    batch_lookback_days: int

    # --- Observability ------------------------------------------------------------
    pushgateway_url: str
    log_level: str

    currency: str

    # ---- derived paths ---------------------------------------------------------
    @property
    def state_dir(self) -> str:
        return os.path.join(self.data_dir, "state")

    @property
    def lake_dir(self) -> str:
        return os.path.join(self.data_dir, "lake")

    @property
    def raw_readings_path(self) -> str:
        return os.path.join(self.lake_dir, "raw", "meter_readings")

    @property
    def reference_dir(self) -> str:
        return os.path.join(self.lake_dir, "reference")

    @property
    def curated_dir(self) -> str:
        return os.path.join(self.lake_dir, "curated")

    @property
    def landing_dir(self) -> str:
        return os.path.join(self.data_dir, "landing")

    @property
    def quarantine_dir(self) -> str:
        return os.path.join(self.data_dir, "landing", "quarantine")

    @property
    def checkpoint_dir(self) -> str:
        return os.path.join(self.data_dir, "checkpoints")

    @property
    def output_dir(self) -> str:
        return os.getenv("OUTPUT_DIR", os.path.join(self.data_dir, "output"))

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user} password={self.postgres_password}"
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def readings_per_day(self) -> int:
        return (24 * 60) // self.interval_minutes


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        kafka_bootstrap=_str("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        topic_meter_readings=_str("TOPIC_METER_READINGS", "meter-readings"),
        topic_dlq=_str("TOPIC_DLQ", "meter-readings-dlq"),
        topic_alerts=_str("TOPIC_ALERTS", "grid-alerts"),
        sim_start=_str("SIM_START", "2026-03-01T00:00:00+00:00"),
        sim_day_seconds=_float("SIM_DAY_SECONDS", 300.0),
        interval_minutes=_int("INTERVAL_MINUTES", 15),
        num_households=_int("NUM_HOUSEHOLDS", 250),
        random_seed=_int("RANDOM_SEED", 42),
        duplicate_rate=_float("DUPLICATE_RATE", 0.01),
        invalid_rate=_float("INVALID_RATE", 0.005),
        outage_rate=_float("OUTAGE_RATE", 0.0012),
        outage_max_hours=_int("OUTAGE_MAX_HOURS", 6),
        tariff_bad_row_rate=_float("TARIFF_BAD_ROW_RATE", 0.01),
        tariff_late_probability=_float("TARIFF_LATE_PROBABILITY", 0.15),
        data_dir=_str("DATA_DIR", "/data"),
        postgres_host=_str("POSTGRES_HOST", "postgres"),
        postgres_port=_int("POSTGRES_PORT", 5432),
        postgres_db=_str("POSTGRES_DB", "smartgrid"),
        postgres_user=_str("POSTGRES_USER", "smartgrid"),
        postgres_password=_str("POSTGRES_PASSWORD", "smartgrid"),
        watermark_sim_minutes=_int("WATERMARK_SIM_MINUTES", 120),
        zone_window_sim_minutes=_int("ZONE_WINDOW_SIM_MINUTES", 60),
        stream_trigger_seconds=_int("STREAM_TRIGGER_SECONDS", 10),
        archive_trigger_seconds=_int("ARCHIVE_TRIGGER_SECONDS", 20),
        low_renewable_threshold=_float("LOW_RENEWABLE_THRESHOLD", 0.25),
        zone_capacity_kw_per_household=_float("ZONE_CAPACITY_KW_PER_HOUSEHOLD", 1.8),
        trace_sample_pct=_int("TRACE_SAMPLE_PCT", 1),
        batch_grace_sim_minutes=_int("BATCH_GRACE_SIM_MINUTES", 60),
        batch_lookback_days=_int("BATCH_LOOKBACK_DAYS", 3),
        pushgateway_url=_str("PUSHGATEWAY_URL", "pushgateway:9091"),
        log_level=_str("LOG_LEVEL", "INFO"),
        currency=_str("CURRENCY", "LKR"),
    )
