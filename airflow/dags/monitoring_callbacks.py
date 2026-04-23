from datetime import datetime, timezone

from prometheus_client import Gauge

from utils_global.logger import get_logger
from utils_global.prometheus import build_registry, metric_name, push_registry


logger = get_logger(__name__)


def _resolve_duration_seconds(context) -> float:
    dag_run = context.get("dag_run")
    if dag_run is None or dag_run.start_date is None:
        return 0.0

    start = dag_run.start_date
    end = dag_run.end_date or datetime.now(timezone.utc)

    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    return float(max((end - start).total_seconds(), 0.0))


def _push_dag_metrics(context, status: str) -> None:
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", None) or getattr(dag_run, "dag_id", "unknown_dag")

    registry = build_registry()

    status_metric = Gauge(
        metric_name("airflow_dag_last_run_status"),
        "Status of the latest DAG run (1 for current status, 0 for opposite status).",
        ["dag_id", "status"],
        registry=registry,
    )
    duration_metric = Gauge(
        metric_name("airflow_dag_duration_seconds"),
        "Duration in seconds for the latest DAG run.",
        ["dag_id"],
        registry=registry,
    )

    duration_seconds = _resolve_duration_seconds(context)

    status_metric.labels(dag_id=dag_id, status="success").set(1.0 if status == "success" else 0.0)
    status_metric.labels(dag_id=dag_id, status="failure").set(1.0 if status == "failure" else 0.0)
    duration_metric.labels(dag_id=dag_id).set(duration_seconds)

    push_registry(
        registry,
        job_name="airflow_monitoring",
        grouping_key={"dag_id": dag_id},
    )

    logger.info("Pushed Airflow DAG metrics for dag_id=%s status=%s", dag_id, status)


def dag_success_callback(context) -> None:
    _push_dag_metrics(context, status="success")


def dag_failure_callback(context) -> None:
    _push_dag_metrics(context, status="failure")
