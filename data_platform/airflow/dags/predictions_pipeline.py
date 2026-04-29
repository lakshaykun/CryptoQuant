# airflow/dags/predictions_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils import check_pipeline_var_enabled


def build_spark_submit(task_script):
    return f"""
    docker exec cryptoquant-spark-1 spark-submit \
      --master local[1] \
    --packages io.delta:delta-spark_2.12:3.1.0 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      /opt/app/{task_script}
    """

with DAG(
    dag_id="predictions_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:
    
    check_pipeline = PythonOperator(
        task_id="check_pipeline",
        python_callable=check_pipeline_var_enabled,
        op_kwargs={"var_name": "predictions_enabled"},
    )

    predict = BashOperator(
        task_id="predict_log_return_lead1",
        bash_command=build_spark_submit(
            "pipelines/jobs/batch/predictions.py"
        ),
        retries=4,
        retry_delay=timedelta(seconds=10),
    )

    check_pipeline >> predict