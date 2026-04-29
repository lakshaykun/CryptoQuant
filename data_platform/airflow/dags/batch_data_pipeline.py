# airflow/dags/batch_data_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pipelines.jobs.batch.cleanup_raw import cleanup_task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import disable_pipeline_vars, enable_pipeline_vars

def build_spark_submit(task_script):
    return f"""
    docker exec cryptoquant-spark-1 spark-submit \
      --master local[2] \
      --packages io.delta:delta-spark_2.12:3.2.0 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      /opt/app/{task_script}
    """

with DAG(
    dag_id="batch_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:
    
    disable_pipeline_task = PythonOperator(
        task_id="disable_pipeline_task",
        python_callable=disable_pipeline_vars,
        op_kwargs={"vars": ["model_training_enabled", "predictions_enabled"]},
    )

    ingest_historical = BashOperator(
        task_id="ingest_historical",
        bash_command=build_spark_submit(
            "pipelines/jobs/batch/ingest.py"
        ),
        retries=3,
        retry_delay=timedelta(seconds=10),
    )

    ingest_today = BashOperator(
        task_id="ingest_today",
        bash_command=build_spark_submit(
            "pipelines/jobs/batch/ingest_today.py"
        ),
        retries=20,
        retry_delay=timedelta(seconds=20),
    )

    bronze = BashOperator(
        task_id="bronze",
        bash_command=build_spark_submit(
            "pipelines/jobs/batch/bronze.py"
        ),
        retries=3,
        retry_delay=timedelta(seconds=10),
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=build_spark_submit(
            "pipelines/jobs/batch/silver.py"
        ),
        retries=3,
        retry_delay=timedelta(seconds=10),
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=build_spark_submit(
            "pipelines/jobs/batch/gold.py"
        ),
        retries=3,
        retry_delay=timedelta(seconds=10),
    )

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup_task,
        retries=3,
        retry_delay=timedelta(seconds=10),
    )

    enable_model_training_task = PythonOperator(
        task_id="enable_model_training_task",
        python_callable=enable_pipeline_vars,
        op_kwargs={"vars": ["model_training_enabled"]},
    )

    trigger_training = TriggerDagRunOperator(
        task_id="trigger_training",
        trigger_dag_id="model_training_pipeline",
    )

    disable_pipeline_task >> (ingest_historical, ingest_today) >> bronze >> silver >> gold 
    gold >> (cleanup, enable_model_training_task)
    enable_model_training_task >> trigger_training