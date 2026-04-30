from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from pipelines.jobs.sentiment.config import load_sentiment_pipeline_config


logger = logging.getLogger(__name__)


def _load_sources() -> list[str]:
    try:
        config = load_sentiment_pipeline_config("streaming")
        sources = list(config.sources)
        if not sources:
            raise ValueError("empty sources from config")
        return sources
    except Exception:
        logger.exception("Failed to load sentiment streaming config during DAG parse; using fallback sources")
        return ["telegram", "youtube", "reddit", "news"]


SOURCES = _load_sources()


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
    for source in SOURCES:
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
