from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils_global.config_loader import load_config


def run_drift_monitor_wrapper():
    from models.monitoring.drift import run_drift_monitor_job

    run_drift_monitor_job()


model_config = load_config("configs/model.yaml") or {}
monitoring_cfg = model_config.get("monitoring") or {}
interval_minutes = int((monitoring_cfg.get("scheduler") or {}).get("interval_minutes", 10))


with DAG(
    dag_id="drift_monitor_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=interval_minutes),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:
    monitor_drift = PythonOperator(
        task_id="monitor_and_trigger_retraining",
        python_callable=run_drift_monitor_wrapper,
        execution_timeout=timedelta(minutes=max(interval_minutes, 5)),
    )
