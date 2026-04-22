from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


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
    dag_id="batch_predictions_pipeline",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    predict = BashOperator(
        task_id="predict_log_return_lead1",
        bash_command=build_spark_submit(
            "pipelines/jobs/batch/predictions.py"
        ),
        retries=4,
        retry_delay=timedelta(minutes=2),
    )