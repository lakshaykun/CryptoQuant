# airflow/dags/model_training_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# Task wrapper functions to avoid import issues in Airflow
def validate_schema_task_wrapper():
    from models.data.validater import validate_schema
    validate_schema()

def train_model_task_wrapper():
    from models.training.train import train_model
    train_model()

def load_data_task_wrapper():
    from models.data.loader import load_data
    load_data()

def feature_engineering_task_wrapper():
    from models.features.feature_engineering import feature_engineering
    feature_engineering()

with DAG(
    dag_id="model_training_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data_task_wrapper
    )

    validate_schema_task = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema_task_wrapper
    )

    feature_engineering_task = PythonOperator(
        task_id="feature_engineering",
        python_callable=feature_engineering_task_wrapper
    )
    
    train_model_task = PythonOperator(
        task_id="model_training",
        python_callable=train_model_task_wrapper,
        execution_timeout=timedelta(minutes=20),
    )

    load_data_task >> validate_schema_task >> feature_engineering_task >> train_model_task