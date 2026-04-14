# airflow/dags/batch_data_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


def build_spark_submit(task_script):
    return f"""
    docker exec spark spark-submit \
      --master local[*] \
      --packages io.delta:delta-spark_2.12:3.0.0 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      /opt/app/{task_script}
    """

with DAG(
    dag_id="batch_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command=build_spark_submit("pipelines/jobs/batch/ingest.py"),
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=build_spark_submit(
            "pipelines/jobs/batch/silver.py"
        ),
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=build_spark_submit(
            "pipelines/jobs/batch/gold.py"
        ),
    )

    ingest >> silver >> gold