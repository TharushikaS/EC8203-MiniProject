"""STREAMING SOURCE - smart-meter simulator publishing to Kafka.

Every ``INTERVAL_MINUTES`` of simulated time (15 min -> ~3.1 real seconds by default)
each household's meter publishes one interval reading to the ``meter-readings`` topic:

    {event_id, schema_version, meter_id, household_id, grid_zone, timestamp,
     interval_minutes, power_consumption_kwh, solar_generation_kwh, voltage_v, produced_at}

``timestamp`` is the simulated *event time* (end of the interval); ``produced_at`` is the
real wall-clock time the record left the producer (used for end-to-end latency tracing).

Kafka design:
  * key = household_id  -> all readings of a meter land on the same partition, so
    per-meter ordering is preserved while load spreads across 6 partitions.
  * acks=all + enable.idempotence -> no loss / no broker-side duplicates on retry.

Realistic faults are injected on purpose so that downstream layers have something to handle:
  * DUPLICATES      - a reading is re-sent with the same event_id (at-least-once retry).
  * INVALID records - negative / impossible kWh, missing household_id, future timestamp
                      (meter clock skew) or a non-JSON payload.
  * OUTAGES / LATE  - a meter loses connectivity for 1..N simulated hours, buffers its
                      readings, then flushes them late. These arrive after the speed layer's
                      watermark and are exactly the data the batch layer later corrects for.
"""
from __future__ import annotations

import json
import random
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient
from prometheus_client import Counter, Gauge, start_http_server

from smartgrid.common.config import Settings, get_settings
from smartgrid.common.energy_model import interval_consumption_kwh, interval_solar_kwh
from smartgrid.common.households import Household, build_registry
from smartgrid.common.logging_utils import get_logger
from smartgrid.common.sim_clock import SimClock, get_clock

SCHEMA_VERSION = 1
log = get_logger("meter-simulator", "ingestion")

EVENTS_SENT = Counter("smartgrid_producer_events_sent_total", "Readings acknowledged by Kafka", ["kind"])
DELIVERY_ERRORS = Counter("smartgrid_producer_delivery_errors_total", "Readings Kafka failed to acknowledge")
FAULTS = Counter("smartgrid_producer_faults_injected_total", "Deliberately injected faults", ["fault"])
METERS_OFFLINE = Gauge("smartgrid_producer_meters_offline", "Meters currently in a simulated outage")
SIM_TIME = Gauge("smartgrid_producer_sim_time_seconds", "Simulated event time of the last emitted interval")
LAST_SEND = Gauge("smartgrid_producer_last_send_unixtime", "Real time of the last successful send")


@dataclass
class MeterState:
    offline_until: datetime | None = None
    buffer: list[dict] = field(default_factory=list)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_reading(h: Household, interval_end: datetime, settings: Settings, rng: random.Random) -> dict:
    interval_start = interval_end - timedelta(minutes=settings.interval_minutes)
    hour_float = interval_start.hour + interval_start.minute / 60.0 + settings.interval_minutes / 120.0
    return {
        "event_id": str(uuid.uuid4()),
        "schema_version": SCHEMA_VERSION,
        "meter_id": h.meter_id,
        "household_id": h.household_id,
        "grid_zone": h.grid_zone,
        "timestamp": iso(interval_end),
        "interval_minutes": settings.interval_minutes,
        "power_consumption_kwh": interval_consumption_kwh(h, hour_float, settings.interval_minutes, rng),
        "solar_generation_kwh": interval_solar_kwh(h, interval_start.date(), hour_float,
                                                   settings.interval_minutes, settings.random_seed, rng),
        "voltage_v": round(rng.gauss(230.0, 3.5), 1),
    }


def corrupt(reading: dict, rng: random.Random) -> tuple[dict | str, str]:
    """Return a corrupted copy of a reading and the name of the fault injected."""
    fault = rng.choice(["negative_kwh", "impossible_kwh", "missing_household", "future_timestamp", "malformed"])
    bad = dict(reading)
    if fault == "negative_kwh":
        bad["power_consumption_kwh"] = -abs(bad["power_consumption_kwh"]) - 0.5
    elif fault == "impossible_kwh":
        bad["power_consumption_kwh"] = round(rng.uniform(60, 500), 2)
    elif fault == "missing_household":
        bad["household_id"] = None
    elif fault == "future_timestamp":
        ts = datetime.fromisoformat(bad["timestamp"].replace("Z", "+00:00"))
        bad["timestamp"] = iso(ts + timedelta(days=rng.randint(2, 30)))
    else:
        return "{corrupted-frame:" + reading["event_id"][:8], fault
    return bad, fault


class MeterSimulator:
    def __init__(self, settings: Settings, clock: SimClock, producer: Producer):
        self.settings = settings
        self.clock = clock
        self.producer = producer
        self.registry = build_registry(settings.num_households, settings.random_seed)
        self.rng = random.Random(settings.random_seed + int(time.time()))
        self.state = {h.household_id: MeterState() for h in self.registry}
        self.running = True
        step = timedelta(minutes=settings.interval_minutes)
        now = clock.now()
        # Resume at the current simulated interval (no historical backfill on restart).
        floored = now.replace(minute=(now.minute // settings.interval_minutes) * settings.interval_minutes,
                              second=0, microsecond=0)
        self.next_interval_end = max(floored, clock.sim_start + step)
        self.step = step

    # -- Kafka ------------------------------------------------------------------
    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            DELIVERY_ERRORS.inc()
            log.error("kafka_delivery_failed", error=str(err), topic=msg.topic(),
                      key=(msg.key() or b"").decode(errors="replace"))
        else:
            LAST_SEND.set(time.time())

    def _send(self, payload: dict | str, key: str | None, kind: str) -> None:
        if isinstance(payload, dict):
            payload = dict(payload, produced_at=iso(datetime.now(timezone.utc)))
            value = json.dumps(payload).encode()
        else:
            value = payload.encode()
        while True:
            try:
                self.producer.produce(self.settings.topic_meter_readings, value=value,
                                      key=key.encode() if key else None, on_delivery=self._on_delivery)
                EVENTS_SENT.labels(kind=kind).inc()
                break
            except BufferError:          # local queue full -> let librdkafka drain, then retry
                self.producer.poll(0.5)
        self.producer.poll(0)

    # -- simulation ----------------------------------------------------------------
    def emit_interval(self, interval_end: datetime) -> dict:
        s, rng = self.settings, self.rng
        stats = {"sent": 0, "buffered": 0, "flushed_late": 0, "duplicates": 0, "invalid": 0}
        for h in self.registry:
            st = self.state[h.household_id]
            reading = build_reading(h, interval_end, s, rng)

            if st.offline_until is None and rng.random() < s.outage_rate:
                hours = rng.randint(1, s.outage_max_hours)
                st.offline_until = interval_end + timedelta(hours=hours)
                FAULTS.labels(fault="meter_outage").inc()
                log.warning("meter_outage_started", household_id=h.household_id, grid_zone=h.grid_zone,
                            sim_time=iso(interval_end), outage_sim_hours=hours)

            if st.offline_until is not None:
                if interval_end < st.offline_until:
                    st.buffer.append(reading)
                    stats["buffered"] += 1
                    continue
                # Connectivity restored: flush the buffered backlog (these are LATE events).
                for old in st.buffer:
                    self._send(old, h.household_id, "late")
                stats["flushed_late"] += len(st.buffer)
                log.info("meter_outage_recovered", household_id=h.household_id,
                         late_readings_flushed=len(st.buffer), sim_time=iso(interval_end))
                st.buffer.clear()
                st.offline_until = None

            if rng.random() < s.invalid_rate:
                bad, fault = corrupt(reading, rng)
                FAULTS.labels(fault=fault).inc()
                self._send(bad, h.household_id, "invalid")
                stats["invalid"] += 1
                continue

            self._send(reading, h.household_id, "normal")
            stats["sent"] += 1
            if rng.random() < s.duplicate_rate:
                FAULTS.labels(fault="duplicate").inc()
                self._send(reading, h.household_id, "duplicate")
                stats["duplicates"] += 1

        METERS_OFFLINE.set(sum(1 for st in self.state.values() if st.offline_until is not None))
        SIM_TIME.set(interval_end.timestamp())
        return stats

    def run(self) -> None:
        log.info("simulator_started", households=len(self.registry), topic=self.settings.topic_meter_readings,
                 sim_day_seconds=self.settings.sim_day_seconds, interval_minutes=self.settings.interval_minutes,
                 first_interval=iso(self.next_interval_end))
        intervals = 0
        while self.running:
            if self.clock.now() >= self.next_interval_end:
                stats = self.emit_interval(self.next_interval_end)
                intervals += 1
                # Log a heartbeat once per simulated hour rather than every interval.
                if self.next_interval_end.minute == 0:
                    log.info("interval_batch_published", sim_time=iso(self.next_interval_end), **stats)
                self.next_interval_end += self.step
                # If we fell behind (e.g. container paused) skip ahead instead of bursting.
                if self.clock.now() - self.next_interval_end > timedelta(hours=2):
                    log.warning("simulator_fell_behind_skipping", from_sim=iso(self.next_interval_end))
                    now = self.clock.now()
                    self.next_interval_end = now.replace(minute=0, second=0, microsecond=0) + self.step
            else:
                self.producer.poll(0.1)
        remaining = self.producer.flush(10)
        log.info("simulator_stopped", intervals_emitted=intervals, undelivered=remaining)


def wait_for_topic(settings: Settings, timeout_s: int = 180) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            md = admin.list_topics(timeout=5)
            if settings.topic_meter_readings in md.topics:
                parts = len(md.topics[settings.topic_meter_readings].partitions)
                log.info("kafka_topic_ready", topic=settings.topic_meter_readings, partitions=parts)
                return
        except KafkaException as exc:
            log.warning("kafka_not_ready", error=str(exc))
        time.sleep(3)
    raise SystemExit(f"Topic {settings.topic_meter_readings} not available after {timeout_s}s")


def main() -> None:
    settings = get_settings()
    start_http_server(8000)
    wait_for_topic(settings)
    clock = get_clock(settings)
    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "client.id": "meter-simulator",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 50,
        "batch.num.messages": 1000,
    })
    sim = MeterSimulator(settings, clock, producer)

    def _stop(*_):
        sim.running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    sim.run()


if __name__ == "__main__":
    main()
