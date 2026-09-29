"""SPEED LAYER + master-dataset writer (Spark Structured Streaming).

One Spark application runs two streaming queries over the ``meter-readings`` topic:

  1. raw_ingest   (stateless, every 20 s)
       * appends EVERY record to the immutable Parquet master dataset, partitioned by
         event_date. No filtering, no dedup, no watermark, so the batch layer can always
         recompute the truth from scratch (Lambda's append-only master dataset);
       * validates each record, counts data quality, sends rejects to the dead-letter topic;
       * samples ~1 % of events for end-to-end tracing.

  2. grid_state   (stateful, every 10 s)
       valid readings -> watermark (2 simulated hours) -> dropDuplicatesWithinWatermark(event_id)
       -> 1-hour tumbling event-time windows per household (update mode).
       The sink rolls these up into zone load / solar / renewable share and per-household
       daily running totals, and raises LOW_RENEWABLE / ZONE_OVERLOAD alerts.

grid_state is deliberately APPROXIMATE: readings that arrive after the watermark (meters
that were offline and flush hours later) are dropped from the real-time view. The batch
layer recomputes from the complete master dataset and the reconciliation measures the gap.

Why only two queries: every streaming query pays a fixed per-trigger cost (Kafka fetch,
planning, state and offset commits). One stateful query at the finest useful grain
(household x hour), with cheap SQL rollups in the serving store, keeps the speed layer
responsive on a laptop while giving the same results as separate zone and household
aggregations.
"""
from __future__ import annotations

import os

from prometheus_client import start_http_server
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from smartgrid.common.config import get_settings
from smartgrid.common.logging_utils import get_logger
from smartgrid.common.sim_clock import get_clock
from smartgrid.speed import transforms as TX
from smartgrid.speed.metrics import PrometheusProgressListener
from smartgrid.speed.sinks import GridStateSink, RawIngestSink

log = get_logger("spark-speed-layer", "processing", name="speed.job")

STATE_STORES = {
    "hdfs": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    "rocksdb": "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("smartgrid-speed-layer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "2"))
        # State store: the default HDFS-backed store is fastest for the demo's small state
        # (~250 households x a few open windows). At production scale set STATE_STORE=rocksdb
        # to keep state off the JVM heap.
        .config("spark.sql.streaming.stateStore.providerClass", STATE_STORES[os.getenv("STATE_STORE", "hdfs")])
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main() -> None:
    settings = get_settings()
    clock = get_clock(settings)
    start_http_server(9108)                        # Prometheus scrape endpoint for the driver
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(PrometheusProgressListener())
    ckpt = settings.checkpoint_dir

    log.info("speed_layer_starting", topic=settings.topic_meter_readings,
             watermark_sim_minutes=settings.watermark_sim_minutes, window_sim_minutes=settings.zone_window_sim_minutes,
             sim_clock=clock.as_dict())

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.topic_meter_readings)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000"))
        .option("failOnDataLoss", "false")
        .load()
    )
    checked = TX.add_quality_columns(TX.parse_kafka_records(kafka, clock), settings.watermark_sim_minutes)

    # 1) Master dataset + data quality + DLQ + tracing.
    (checked.writeStream.queryName("raw_ingest")
     .foreachBatch(RawIngestSink(settings))
     .option("checkpointLocation", f"{ckpt}/raw_ingest")
     .trigger(processingTime=f"{settings.archive_trigger_seconds} seconds")
     .start())

    # 2) Real-time grid state: dedup within the watermark, then 1-hour windows per household.
    valid = (
        TX.add_interval_energy(checked.where(F.col("invalid_reason").isNull()))
        .withWatermark("interval_start_ts", f"{settings.watermark_sim_minutes} minutes")
        .dropDuplicatesWithinWatermark(["event_id"])       # at-least-once re-sends from meters
    )
    grid = (
        valid.groupBy(F.window("interval_start_ts", f"{settings.zone_window_sim_minutes} minutes"),
                      "household_id", "grid_zone")
        .agg(F.sum("power_consumption_kwh").alias("consumption_kwh"),
             F.sum("solar_generation_kwh").alias("solar_kwh"),
             F.sum("import_kwh").alias("import_kwh"),
             F.sum("export_kwh").alias("export_kwh"),
             F.sum("peak_import_kwh").alias("peak_import_kwh"),
             F.count("*").alias("reading_count"),
             F.max("event_ts").alias("last_event_ts"))
    )
    (grid.writeStream.queryName("grid_state")
     .outputMode("update")
     .foreachBatch(GridStateSink(settings, clock))
     .option("checkpointLocation", f"{ckpt}/grid_state")
     .trigger(processingTime=f"{settings.stream_trigger_seconds} seconds")
     .start())

    log.info("speed_layer_running", queries=[q.name for q in spark.streams.active])
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
