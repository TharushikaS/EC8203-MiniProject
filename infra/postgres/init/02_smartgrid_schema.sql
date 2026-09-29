-- SmartGrid serving store schema.
--   rt_*      : SPEED views, written continuously by Spark Structured Streaming (approximate, low latency)
--   batch_*   : BATCH views, rewritten by the Airflow-orchestrated Spark batch job (authoritative)
--   reference_*: validated daily feeds (tariff + weather)
--   pipeline/ops tables: observability (traces, quality stats, run history, alert log)
\connect smartgrid
SET ROLE smartgrid;
SET TIME ZONE 'UTC';

-- ============================================================ SPEED LAYER VIEWS
CREATE TABLE rt_zone_metrics (
    grid_zone           TEXT             NOT NULL,
    window_start        TIMESTAMPTZ      NOT NULL,   -- simulated event time
    window_end          TIMESTAMPTZ      NOT NULL,
    consumption_kwh     DOUBLE PRECISION NOT NULL,
    solar_kwh           DOUBLE PRECISION NOT NULL,
    load_kw             DOUBLE PRECISION NOT NULL,   -- average load over the observed part of the window
    solar_kw            DOUBLE PRECISION NOT NULL,
    renewable_share     DOUBLE PRECISION NOT NULL,   -- min(solar, load) / load
    reading_count       INTEGER          NOT NULL,
    active_meters       INTEGER          NOT NULL,
    expected_meters     INTEGER          NOT NULL,
    intervals_observed  DOUBLE PRECISION NOT NULL,
    window_start_real   TIMESTAMPTZ      NOT NULL,   -- real wall-clock time of window_start (for Grafana time axis)
    updated_at          TIMESTAMPTZ      NOT NULL,
    micro_batch_id      BIGINT,
    PRIMARY KEY (grid_zone, window_start)
);
CREATE INDEX rt_zone_metrics_ws ON rt_zone_metrics (window_start DESC);
CREATE INDEX rt_zone_metrics_real ON rt_zone_metrics (window_start_real DESC);

-- Finest-grained speed view: Spark's stateful output (household x 1-hour event-time window).
-- rt_zone_metrics and rt_household_daily are rolled up from it by the speed-layer sink.
CREATE TABLE rt_household_hourly (
    household_id     TEXT             NOT NULL,
    window_start     TIMESTAMPTZ      NOT NULL,   -- simulated event time
    grid_zone        TEXT             NOT NULL,
    consumption_kwh  DOUBLE PRECISION NOT NULL,
    solar_kwh        DOUBLE PRECISION NOT NULL,
    import_kwh       DOUBLE PRECISION NOT NULL,
    export_kwh       DOUBLE PRECISION NOT NULL,
    peak_import_kwh  DOUBLE PRECISION NOT NULL,
    reading_count    INTEGER          NOT NULL,
    last_event_ts    TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ      NOT NULL,
    micro_batch_id   BIGINT,
    PRIMARY KEY (household_id, window_start)
);
CREATE INDEX rt_household_hourly_ws ON rt_household_hourly (window_start);

CREATE TABLE rt_household_daily (
    sim_date         DATE             NOT NULL,
    household_id     TEXT             NOT NULL,
    grid_zone        TEXT             NOT NULL,
    consumption_kwh  DOUBLE PRECISION NOT NULL,
    solar_kwh        DOUBLE PRECISION NOT NULL,
    import_kwh       DOUBLE PRECISION NOT NULL,
    export_kwh       DOUBLE PRECISION NOT NULL,
    peak_import_kwh  DOUBLE PRECISION NOT NULL,
    reading_count    INTEGER          NOT NULL,
    last_event_ts    TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ      NOT NULL,
    micro_batch_id   BIGINT,
    PRIMARY KEY (household_id, sim_date)
);
CREATE INDEX rt_household_daily_date ON rt_household_daily (sim_date);

CREATE TABLE grid_alerts (
    alert_id          BIGSERIAL PRIMARY KEY,
    alert_type        TEXT             NOT NULL,     -- LOW_RENEWABLE | ZONE_OVERLOAD
    grid_zone         TEXT             NOT NULL,
    window_start      TIMESTAMPTZ      NOT NULL,     -- simulated
    severity          TEXT             NOT NULL,
    metric_value      DOUBLE PRECISION NOT NULL,
    threshold         DOUBLE PRECISION NOT NULL,
    message           TEXT             NOT NULL,
    window_start_real TIMESTAMPTZ,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    UNIQUE (alert_type, grid_zone, window_start)     -- idempotent under micro-batch replay
);
CREATE INDEX grid_alerts_created ON grid_alerts (created_at DESC);

-- ============================================================ REFERENCE DATA (daily batch source)
CREATE TABLE reference_tariffs (
    bill_date        DATE             NOT NULL,
    household_id     TEXT             NOT NULL,
    tariff_plan      TEXT             NOT NULL,
    tariff_rate      DOUBLE PRECISION NOT NULL,
    billing_tier     TEXT             NOT NULL,
    subsidy_flag     BOOLEAN          NOT NULL,
    feed_in_rate     DOUBLE PRECISION NOT NULL,
    carried_forward  BOOLEAN          NOT NULL DEFAULT FALSE,
    tariff_version   INTEGER          NOT NULL DEFAULT 1,
    source_file      TEXT,
    loaded_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (bill_date, household_id)
);

CREATE TABLE reference_weather (
    bill_date                      DATE             NOT NULL,
    grid_zone                      TEXT             NOT NULL,
    cloud_cover                    DOUBLE PRECISION NOT NULL,
    solar_potential_index          DOUBLE PRECISION NOT NULL,
    forecast_next_day_cloud_cover  DOUBLE PRECISION,
    PRIMARY KEY (bill_date, grid_zone)
);

-- ============================================================ BATCH LAYER VIEWS
CREATE TABLE batch_household_daily_bill (
    bill_date          DATE             NOT NULL,
    household_id       TEXT             NOT NULL,
    grid_zone          TEXT             NOT NULL,
    tariff_plan        TEXT             NOT NULL,
    billing_tier       TEXT             NOT NULL,
    subsidy_flag       BOOLEAN          NOT NULL,
    tariff_version     INTEGER,
    carried_forward    BOOLEAN,
    consumption_kwh    DOUBLE PRECISION NOT NULL,
    solar_kwh          DOUBLE PRECISION NOT NULL,
    self_consumed_kwh  DOUBLE PRECISION NOT NULL,
    import_kwh         DOUBLE PRECISION NOT NULL,
    peak_import_kwh    DOUBLE PRECISION NOT NULL,
    export_kwh         DOUBLE PRECISION NOT NULL,
    tariff_rate        DOUBLE PRECISION NOT NULL,
    feed_in_rate       DOUBLE PRECISION NOT NULL,
    energy_charge      DOUBLE PRECISION NOT NULL,
    block_surcharge    DOUBLE PRECISION NOT NULL,
    fixed_charge       DOUBLE PRECISION NOT NULL,
    subsidy_discount   DOUBLE PRECISION NOT NULL,
    feed_in_credit     DOUBLE PRECISION NOT NULL,
    net_amount         DOUBLE PRECISION NOT NULL,
    amount_due         DOUBLE PRECISION NOT NULL,
    carried_credit     DOUBLE PRECISION NOT NULL,
    reading_count      BIGINT           NOT NULL,
    late_readings      BIGINT           NOT NULL,
    data_completeness  DOUBLE PRECISION NOT NULL,
    batch_run_id       TEXT             NOT NULL,
    computed_at        TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (bill_date, household_id)
);
CREATE INDEX batch_bill_household ON batch_household_daily_bill (household_id, bill_date DESC);

CREATE TABLE batch_zone_daily_summary (
    bill_date             DATE             NOT NULL,
    grid_zone             TEXT             NOT NULL,
    households            BIGINT           NOT NULL,
    consumption_kwh       DOUBLE PRECISION NOT NULL,
    solar_kwh             DOUBLE PRECISION NOT NULL,
    export_kwh            DOUBLE PRECISION NOT NULL,
    renewable_share       DOUBLE PRECISION NOT NULL,
    peak_load_kw          DOUBLE PRECISION,
    peak_hour             INTEGER,
    cloud_cover           DOUBLE PRECISION,
    total_billed          DOUBLE PRECISION NOT NULL,
    avg_bill              DOUBLE PRECISION NOT NULL,
    total_feed_in_credit  DOUBLE PRECISION NOT NULL,
    batch_run_id          TEXT             NOT NULL,
    computed_at           TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (bill_date, grid_zone)
);

CREATE TABLE batch_zone_hourly (
    bill_date        DATE             NOT NULL,
    hour_of_day      INTEGER          NOT NULL,
    grid_zone        TEXT             NOT NULL,
    consumption_kwh  DOUBLE PRECISION NOT NULL,
    solar_kwh        DOUBLE PRECISION NOT NULL,
    renewable_share  DOUBLE PRECISION NOT NULL,
    batch_run_id     TEXT             NOT NULL,
    computed_at      TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (bill_date, hour_of_day, grid_zone)
);

CREATE TABLE speed_batch_reconciliation (
    bill_date              DATE             NOT NULL,
    grid_zone              TEXT             NOT NULL,
    speed_consumption_kwh  DOUBLE PRECISION NOT NULL,
    batch_consumption_kwh  DOUBLE PRECISION NOT NULL,
    speed_solar_kwh        DOUBLE PRECISION NOT NULL,
    batch_solar_kwh        DOUBLE PRECISION NOT NULL,
    drift_kwh              DOUBLE PRECISION NOT NULL,
    drift_pct              DOUBLE PRECISION,
    late_records           BIGINT           NOT NULL DEFAULT 0,
    computed_at            TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (bill_date, grid_zone)
);

-- ============================================================ OBSERVABILITY / LINEAGE
CREATE TABLE batch_runs (
    run_id                     TEXT        NOT NULL,
    bill_date                  DATE        NOT NULL,
    run_type                   TEXT        NOT NULL,   -- daily | recompute | manual
    status                     TEXT        NOT NULL,   -- SUCCESS | FAILED
    raw_records                BIGINT,
    invalid_records            BIGINT,
    duplicates_removed         BIGINT,
    valid_records              BIGINT,
    late_records               BIGINT,
    households_billed          BIGINT,
    households_without_tariff  BIGINT,
    total_billed               DOUBLE PRECISION,
    tariff_version             INTEGER,
    invalid_reasons            JSONB,
    duration_seconds           DOUBLE PRECISION,
    error                      TEXT,
    started_at                 TIMESTAMPTZ,
    finished_at                TIMESTAMPTZ,
    PRIMARY KEY (run_id, bill_date)
);
CREATE INDEX batch_runs_date ON batch_runs (bill_date, status);

CREATE TABLE pipeline_event_trace (
    event_id            TEXT PRIMARY KEY,
    household_id        TEXT,
    grid_zone           TEXT,
    event_ts            TIMESTAMPTZ,       -- simulated event time
    produced_at         TIMESTAMPTZ,       -- left the producer (real)
    kafka_ts            TIMESTAMPTZ,       -- appended to Kafka log (real)
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    speed_processed_at  TIMESTAMPTZ,       -- processed by the speed layer (real)
    e2e_latency_ms      DOUBLE PRECISION,
    is_late             BOOLEAN,
    micro_batch_id      BIGINT
);
CREATE INDEX pipeline_event_trace_processed ON pipeline_event_trace (speed_processed_at DESC);

CREATE TABLE stream_quality_stats (
    micro_batch_id   BIGINT PRIMARY KEY,
    processed_at     TIMESTAMPTZ NOT NULL,
    valid_count      BIGINT      NOT NULL,
    invalid_count    BIGINT      NOT NULL,
    late_count       BIGINT      NOT NULL,
    invalid_reasons  JSONB
);
CREATE INDEX stream_quality_stats_ts ON stream_quality_stats (processed_at DESC);

CREATE TABLE ops_alerts (
    id           BIGSERIAL PRIMARY KEY,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    alertname    TEXT        NOT NULL,
    status       TEXT        NOT NULL,         -- firing | resolved
    severity     TEXT,
    summary      TEXT,
    labels       JSONB,
    starts_at    TIMESTAMPTZ,
    ends_at      TIMESTAMPTZ
);
CREATE INDEX ops_alerts_received ON ops_alerts (received_at DESC);

-- Serving-layer convenience view: the most recent window per zone (the "right now" view).
CREATE VIEW v_zone_latest AS
SELECT DISTINCT ON (grid_zone) *
FROM rt_zone_metrics
ORDER BY grid_zone, window_start DESC;

RESET ROLE;
GRANT CONNECT ON DATABASE smartgrid TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE smartgrid IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;
