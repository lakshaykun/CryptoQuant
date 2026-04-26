from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from pipelines.jobs.sentiment.config import load_sentiment_pipeline_config

CONFIG = load_sentiment_pipeline_config("streaming")


def build_python_job(module: str, args: str = "") -> str:
    return f"""
    cd /opt/app && PYTHONPATH=/opt/app python3 -m {module} {args}
    """


with DAG(
    dag_id="sentiment_streaming_pipeline",
    start_date=datetime(2024, 1, 1),
    # Streaming-style cadence: every 1 minute, each run ingests the last X minutes
    # from config (configs/data.yaml -> sentiment.streaming_lookback_minutes).
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    preflight = BashOperator(
        task_id="preflight_streaming",
        bash_command=build_python_job(
            "pipelines.jobs.preflight.sentiment",
            "--mode streaming",
        ),
    )
    ingest_tasks = []
    for source in CONFIG.sources:
        ingest_tasks.append(
            BashOperator(
                task_id=f"ingest_{source}_streaming",
                bash_command=build_python_job(
                    "pipelines.jobs.batch.sentiment",
                    f"--stage ingest --mode streaming --source {source}",
                ),
            )
        )

    bronze = BashOperator(
        task_id="bronze_delta_lake_streaming",
        bash_command=build_python_job(
            "pipelines.jobs.batch.sentiment",
            "--stage bronze --mode streaming",
        ),
    )
    silver = BashOperator(
        task_id="silver_delta_lake_streaming",
        bash_command=build_python_job(
            "pipelines.jobs.batch.sentiment",
            "--stage silver --mode streaming",
        ),
    )
    gold = BashOperator(
        task_id="gold_delta_lake_streaming",
        bash_command=build_python_job(
            "pipelines.jobs.batch.sentiment",
            "--stage gold --mode streaming",
        ),
    )

    preflight >> ingest_tasks >> bronze >> silver >> gold
