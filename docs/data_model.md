# Data model: sources, topics, lake and serving tables

## 1. Streaming source: Kafka topic `meter-readings`

6 partitions, key = `household_id`, retention 7 days, producer `acks=all` + idempotence + lz4.

| Field | Type | Notes |
|---|---|---|
| `event_id` | UUID string | unique per reading; the trace id; re-sent unchanged on duplicates |
| `schema_version` | int | `1` |
| `meter_id`, `household_id` | string | `MTR-00001`, `HH-00001` |
| `grid_zone` | string | `COLOMBO`, `GALLE`, `KANDY`, `MATARA`, `JAFFNA` |
| `timestamp` | ISO-8601 UTC | **simulated event time = end of the 15-min interval** |
| `interval_minutes` | int | 15 |
| `power_consumption_kwh` | double | energy used in the interval |
| `solar_generation_kwh` | double | rooftop PV generation in the interval (0 at night / no panels) |
| `voltage_v` | double | informational |
| `produced_at` | ISO-8601 UTC | real wall-clock send time (latency tracing) |

Derived during processing: `interval_start_ts` (event time used for windows, days and hours),
`sim_ingest_ts` (simulated time at Kafka append), `invalid_reason`, `is_late`, `import_kwh`,
`export_kwh`, `self_consumed_kwh`, `is_peak`.

**Validation rules** (shared by both layers, `speed/transforms.py`), in order:
`malformed_json` → `missing_household` → `bad_timestamp` → `unknown_zone` → `negative_kwh` →
`impossible_kwh` (> 10 kWh per interval) → `future_timestamp` (> 1 simulated hour after ingestion).

Other topics: `meter-readings-dlq` (rejected records with their reason and original payload) and
`grid-alerts` (LOW_RENEWABLE / ZONE_OVERLOAD alerts, key = zone).

## 2. Daily batch source: landing zone `/data/landing`

`tariffs/tariffs_<YYYY-MM-DD>.csv` (corrections arrive as `tariffs_<date>.v2.csv`, `.v3` and so on):

| Column | Example | Rule |
|---|---|---|
| `household_id` | HH-00042 | must exist in the customer registry |
| `tariff_plan` | FLAT / TOU / GREEN | known plan |
| `tariff_rate` | 31.25 | LKR per kWh, 0 < rate ≤ 200; changes daily (fuel adjustment) |
| `billing_tier` | LIFELINE / STANDARD / HIGH_USAGE | known tier |
| `subsidy_flag` | true / false | boolean |
| `feed_in_rate` | 22.0 | LKR per exported kWh, ≥ 0 |
| `effective_date`, `published_at` | | lineage |

`weather/weather_<YYYY-MM-DD>.json`: per zone `cloud_cover`, `solar_potential_index`, and
`forecast_next_day_cloud_cover`.

Rejected rows go to `landing/quarantine/tariffs_<date>_v<n>_rejected.csv` with a `reject_reason` column.

## 3. Data lake: `/data/lake` (Parquet)

| Path | Written by | Partitioning | Content |
|---|---|---|---|
| `raw/meter_readings/` | speed layer `raw_ingest` | `event_date` | **master dataset**: every record as received, including invalid ones and duplicates, plus Kafka partition/offset/timestamp |
| `reference/tariffs/` | batch `load_reference_data` | `bill_date` | validated tariff per household per day (+ `carried_forward`, `tariff_version`) |
| `reference/weather/` | batch `load_reference_data` | `bill_date` | per-zone weather |
| `curated/household_daily_bill/` | batch Spark job | `bill_date` | authoritative bills (same content as the Postgres table) |

## 4. Serving store: PostgreSQL `smartgrid`

### Speed views (continuously updated, approximate)
| Table | Grain | Key columns |
|---|---|---|
| `rt_household_hourly` | household × 1-h window | kWh consumption / solar / import / export / peak import, readings |
| `rt_zone_metrics` | zone × 1-h window | `load_kw`, `solar_kw`, `renewable_share`, `active_meters`/`expected_meters`, `window_start_real` |
| `rt_household_daily` | household × day | running daily totals used for provisional billing |
| `grid_alerts` | alert × zone × window | `LOW_RENEWABLE`, `ZONE_OVERLOAD`, unique per window (idempotent) |

### Reference data
`reference_tariffs` (bill_date, household) and `reference_weather` (bill_date, zone).

### Batch views (authoritative, replaced per day)
| Table | Grain | Highlights |
|---|---|---|
| `batch_household_daily_bill` | household × day | full price breakdown: energy, block surcharge, fixed, subsidy, feed-in credit, `amount_due`, `carried_credit`; lineage: `batch_run_id`, `tariff_version`, `reading_count`, `late_readings`, `data_completeness` |
| `batch_zone_daily_summary` | zone × day | consumption, solar, renewable share, peak load and hour, cloud cover, total billed, average bill |
| `batch_zone_hourly` | zone × hour-of-day × day | load profile and renewable share by time of day |
| `speed_batch_reconciliation` | zone × day | speed vs batch kWh, drift kWh and %, late readings |

### Observability tables
`batch_runs` (one row per run and day: raw, invalid, duplicates, valid, late, households, tariff
version, duration, status/error), `stream_quality_stats` (per micro-batch), `pipeline_event_trace`
(sampled stage timestamps), `ops_alerts` (Alertmanager webhook log).

## 5. Billing rules (`common/billing.py`)

```
off-peak import = import − peak import            (peak = 18:00–21:59)
energy charge   = rate × (off-peak × plan.offpeak_mult + peak × plan.peak_mult)
block surcharge = max(0, import − tier.block_limit) × rate × (tier.above_block_mult − 1)
subsidy         = min(25 % × (energy + block), 150 LKR)  if subsidy_flag
feed-in credit  = export × feed_in_rate
net             = tier.fixed_daily_charge + energy + block − subsidy − credit
amount_due      = max(net, 0);  carried_credit = max(−net, 0)
```

| Plan | Peak × | Off-peak × | | Tier | Block limit | Above-block × | Fixed / day |
|---|---|---|---|---|---|---|---|
| FLAT | 1.0 | 1.0 | | LIFELINE | 4 kWh | 1.60 | 10 LKR |
| TOU | 1.8 | 0.8 | | STANDARD | 8 kWh | 1.35 | 25 LKR |
| GREEN | 1.2 | 1.0 | | HIGH_USAGE | 8 kWh | 1.60 | 60 LKR |
