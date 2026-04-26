from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from pipelines.jobs.sentiment.config import load_sentiment_pipeline_config


CONFIG = load_sentiment_pipeline_config("batch")


def build_python_job(module: str, args: str = "") -> str:
    return f"""
    cd /opt/app && PYTHONPATH=/opt/app python3 -m {module} {args}
    """

with DAG(
    dag_id="sentiment_batch_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    preflight = BashOperator(
        task_id="preflight_batch",
        bash_command=build_python_job(
            "pipelines.jobs.preflight.sentiment",
            "--mode batch",
        ),
    )
    ingest_tasks = []
    for source in CONFIG.sources:
        ingest_tasks.append(
            BashOperator(
                task_id=f"ingest_{source}_batch",
                bash_command=build_python_job(
                    "pipelines.jobs.batch.sentiment",
                    f"--stage ingest --mode batch --source {source}",
                ),
            )
        )

    bronze = BashOperator(
        task_id="bronze_delta_lake_batch",
        bash_command=build_python_job(
            "pipelines.jobs.batch.sentiment",
            "--stage bronze --mode batch",
        ),
    )
    silver = BashOperator(
        task_id="silver_delta_lake_batch",
        bash_command=build_python_job(
            "pipelines.jobs.batch.sentiment",
            "--stage silver --mode batch",
        ),
    )
    gold = BashOperator(
        task_id="gold_delta_lake_batch",
        bash_command=build_python_job(
            "pipelines.jobs.batch.sentiment",
            "--stage gold --mode batch",
        ),
    )

    preflight >> ingest_tasks >> bronze >> silver >> gold
