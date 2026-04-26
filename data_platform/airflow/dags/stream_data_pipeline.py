# airflow/dags/stream_data_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


def build_spark_submit(task_script):
    return f"""
    docker exec cryptoquant-spark-1 spark-submit \
        --master local[*] \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/app/{task_script}
    """


with DAG(
    dag_id="stream_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    predict = BashOperator(
        task_id="market_stream",
        bash_command=build_spark_submit(
            "pipelines/jobs/streaming/market.py"
        ),
        retries=2,
        retry_delay=timedelta(seconds=10),
    )