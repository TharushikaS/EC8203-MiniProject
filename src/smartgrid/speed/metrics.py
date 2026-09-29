"""Prometheus metrics for the Spark driver, fed by a StreamingQueryListener.

Spark tracks Kafka offsets in its own checkpoint (not in a Kafka consumer group), so broker
side "consumer lag" tools cannot see it. We derive lag from each micro-batch's progress
report instead: lag = latestOffset (head of the topic) - endOffset (what was processed).
"""
from __future__ import annotations

import json
import time
from datetime import datetime

from prometheus_client import Counter, Gauge, Histogram
from pyspark.sql.streaming import StreamingQueryListener

from smartgrid.common.logging_utils import get_logger

log = get_logger("spark-speed-layer", "processing", name="speed.listener")

INPUT_ROWS = Counter("smartgrid_stream_input_rows_total", "Rows read from Kafka", ["query"])
INPUT_RATE = Gauge("smartgrid_stream_input_rows_per_second", "Input rate", ["query"])
PROCESS_RATE = Gauge("smartgrid_stream_processed_rows_per_second", "Processing rate", ["query"])
BATCH_DURATION = Gauge("smartgrid_stream_batch_duration_ms", "Micro-batch duration", ["query"])
KAFKA_LAG = Gauge("smartgrid_stream_kafka_lag_records", "Records behind the head of the topic", ["query"])
STATE_ROWS = Gauge("smartgrid_stream_state_rows", "Rows held in the state store", ["query"])
LAST_PROGRESS = Gauge("smartgrid_stream_last_progress_unixtime", "Last micro-batch completion", ["query"])
WATERMARK = Gauge("smartgrid_stream_watermark_sim_seconds", "Event-time watermark (simulated epoch s)", ["query"])
QUERY_FAILURES = Counter("smartgrid_stream_query_terminated_total", "Queries that terminated with an error", ["query"])

RECORDS = Counter("smartgrid_stream_records_total", "Records seen by the quality stage", ["status"])
INVALID = Counter("smartgrid_stream_invalid_records_total", "Invalid records by reason", ["reason"])
LATE = Counter("smartgrid_stream_late_records_total", "Records that arrived later than the watermark")
E2E_LATENCY = Histogram("smartgrid_stream_e2e_latency_seconds", "Producer -> speed layer latency (sampled)",
                        buckets=(0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300))
ALERTS_RAISED = Counter("smartgrid_stream_grid_alerts_total", "Business alerts raised by the speed layer", ["type"])
SINK_ROWS = Counter("smartgrid_stream_sink_rows_total", "Rows upserted into the serving store", ["table"])
SINK_ERRORS = Counter("smartgrid_stream_sink_errors_total", "Failed writes to the serving store", ["table"])


def _offset_total(offset_json: str | None) -> int:
    if not offset_json:
        return 0
    try:
        data = json.loads(offset_json)
    except (TypeError, json.JSONDecodeError):
        return 0
    return sum(int(v) for parts in data.values() for v in parts.values())


class PrometheusProgressListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        log.info("stream_query_started", query=event.name, query_id=str(event.id))

    def onQueryProgress(self, event):
        p = event.progress
        name = p.name or "unnamed"
        INPUT_ROWS.labels(query=name).inc(p.numInputRows)
        INPUT_RATE.labels(query=name).set(p.inputRowsPerSecond or 0.0)
        PROCESS_RATE.labels(query=name).set(p.processedRowsPerSecond or 0.0)
        BATCH_DURATION.labels(query=name).set(p.batchDuration or 0)
        LAST_PROGRESS.labels(query=name).set(time.time())
        lag = sum(max(0, _offset_total(s.latestOffset) - _offset_total(s.endOffset)) for s in p.sources)
        KAFKA_LAG.labels(query=name).set(lag)
        STATE_ROWS.labels(query=name).set(sum(op.numRowsTotal for op in p.stateOperators))
        wm = (p.eventTime or {}).get("watermark")
        if wm:
            try:
                WATERMARK.labels(query=name).set(datetime.fromisoformat(wm.replace("Z", "+00:00")).timestamp())
            except ValueError:
                pass
        if p.numInputRows:
            log.info("micro_batch_completed", query=name, batch_id=p.batchId, input_rows=p.numInputRows,
                     duration_ms=p.batchDuration, kafka_lag=lag, watermark=wm, phases_ms=dict(p.durationMs or {}))

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        if event.exception:
            QUERY_FAILURES.labels(query=str(event.id)).inc()
            log.error("stream_query_failed", query_id=str(event.id), error=event.exception[:2000])
        else:
            log.info("stream_query_stopped", query_id=str(event.id))
