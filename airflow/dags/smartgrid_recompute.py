"""BATCH LAYER - on-demand recomputation / backfill (manually triggered).

The defining capability of a Lambda architecture: because the raw archive is immutable and
the batch job is a pure function of (raw data + reference data + code), any range of days
can be recomputed from scratch at any time. Typical triggers:
  * the billing system re-issues a corrected tariff file (tariffs_<date>.v2.csv),
  * a bug fix / rule change in the billing logic,
  * meter readings that arrived after the day was first billed.

Trigger with config, e.g. from the UI ("Trigger DAG w/ config") or CLI:
    airflow dags trigger smartgrid_recompute -c '{"start_date": "2026-03-02", "end_date": "2026-03-03",
                                                  "reason": "tariff correction v2"}'
"""
from __future__ import annotations

from datetime import date, datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from smartgrid.batch import tasks
from smartgrid.batch.reference_loader import load_reference_for_day
from smartgrid.common.logging_utils import get_logger

DAG_ID = "smartgrid_recompute"
log = get_logger("batch-layer", "orchestration", name="dag.recompute")


def _days(params) -> list[str]:
    return tasks.days_in_range(params["start_date"], params["end_date"] or params["start_date"])


def reload_reference(**ctx):
    params = ctx["params"]
    log.info("recompute_requested", start=params["start_date"], end=params["end_date"], reason=params["reason"],
             run_id=ctx["run_id"])
    return [load_reference_for_day(date.fromisoformat(d)) for d in _days(params)]


def reconcile_all(**ctx):
    return [tasks.reconcile_speed_vs_batch(d) for d in _days(ctx["params"])]


def reports_all(**ctx):
    return [tasks.generate_report(d) for d in _days(ctx["params"])]


def metrics_all(**ctx):
    for d in _days(ctx["params"]):
        tasks.publish_batch_metrics(d, DAG_ID, 1)


with DAG(
    dag_id=DAG_ID,
    description="Recompute batch views for a date range from the immutable raw archive",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": Param("2026-03-01", type="string", format="date"),
        "end_date": Param("2026-03-01", type=["null", "string"], format="date"),
        "reason": Param("manual recompute", type="string"),
    },
    default_args={"owner": "smartgrid", "retries": 0},
    tags=["smartgrid", "batch-layer", "lambda", "backfill"],
    doc_md=__doc__,
) as dag:
    reload_ref = PythonOperator(task_id="reload_reference_data", python_callable=reload_reference)
    recompute = BashOperator(
        task_id="recompute_billing",
        bash_command=(
            "spark-submit --master ${SPARK_MASTER:-local[2]} --driver-memory ${SPARK_DRIVER_MEMORY:-1g} "
            "--jars /opt/jars/postgresql.jar --conf spark.ui.enabled=false "
            "/opt/smartgrid/src/smartgrid/batch/batch_billing.py "
            "--start-date {{ params.start_date }} --end-date {{ params.end_date or params.start_date }} "
            "--run-id '{{ run_id }}' --run-type recompute"
        ),
    )
    reconcile = PythonOperator(task_id="reconcile_speed_vs_batch", python_callable=reconcile_all)
    report = PythonOperator(task_id="regenerate_reports", python_callable=reports_all)
    metrics = PythonOperator(task_id="publish_metrics", python_callable=metrics_all)

    reload_ref >> recompute >> reconcile >> report >> metrics
