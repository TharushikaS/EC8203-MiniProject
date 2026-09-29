"""BATCH LAYER orchestration - daily billing pipeline.

Simulated clock: 1 simulated day = SIM_DAY_SECONDS real seconds (default 300 s). The DAG
runs every 2 real minutes and processes at most one *completed* simulated day per run
(the oldest unbilled day in the look-back window), so it keeps up with the compressed
clock and self-heals after downtime.

    resolve_target_day  (short-circuit: nothing to do -> skip everything)
      -> wait_for_tariff_feed      sensor: daily billing extract has landed (may be hours late)
      -> load_reference_data       validate + quarantine + carry-forward, lake + Postgres
      -> run_batch_billing         Spark batch job: full recompute from the raw archive
      -> reconcile_speed_vs_batch  quantify what the speed layer missed
      -> generate_daily_report     consolidated HTML/CSV report
      -> publish_metrics           Pushgateway -> Prometheus (freshness / status alerts)
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.python import PythonSensor

from smartgrid.batch import tasks

DAG_ID = "smartgrid_daily_batch"
TARGET = "{{ ti.xcom_pull(task_ids='resolve_target_day') }}"

SPARK_SUBMIT = (
    "spark-submit --master ${SPARK_MASTER:-local[2]} --driver-memory ${SPARK_DRIVER_MEMORY:-1g} "
    "--jars /opt/jars/postgresql.jar --conf spark.ui.enabled=false "
    "/opt/smartgrid/src/smartgrid/batch/batch_billing.py "
)

default_args = {
    "owner": "smartgrid",
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
    "on_failure_callback": tasks.airflow_failure_callback,
}

with DAG(
    dag_id=DAG_ID,
    description="Daily tariff join, authoritative billing and consolidated report (Lambda batch layer)",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=2),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["smartgrid", "batch-layer", "lambda"],
    doc_md=__doc__,
) as dag:
    resolve = ShortCircuitOperator(
        task_id="resolve_target_day",
        python_callable=tasks.resolve_target_day,
    )

    wait_tariff = PythonSensor(
        task_id="wait_for_tariff_feed",
        python_callable=tasks.tariff_file_ready,
        op_args=[TARGET],
        poke_interval=10,
        timeout=600,                 # ~2 simulated days: after that the feed is declared missing
        mode="reschedule",
    )

    load_ref = PythonOperator(
        task_id="load_reference_data",
        python_callable=tasks.load_reference,
        op_args=[TARGET],
    )

    billing = BashOperator(
        task_id="run_batch_billing",
        bash_command=SPARK_SUBMIT + f"--start-date {TARGET} --run-id '{{{{ run_id }}}}' --run-type daily",
        execution_timeout=timedelta(minutes=10),
    )

    reconcile = PythonOperator(
        task_id="reconcile_speed_vs_batch",
        python_callable=tasks.reconcile_speed_vs_batch,
        op_args=[TARGET],
    )

    report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=tasks.generate_report,
        op_args=[TARGET],
    )

    metrics = PythonOperator(
        task_id="publish_metrics",
        python_callable=tasks.publish_batch_metrics,
        op_args=[TARGET, DAG_ID, 1],
    )

    resolve >> wait_tariff >> load_ref >> billing >> reconcile >> report >> metrics
