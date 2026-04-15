# airflow/dags/model_training_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from models.data.loader import load_data
from models.data.validater import validate_schema
from models.training.train import train_model
from models.features.feature_engineering import feature_engineering

with DAG(
    dag_id="model_training_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    validate_schema_task = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema
    )

    feature_engineering_task = PythonOperator(
        task_id="feature_engineering",
        python_callable=feature_engineering
    )
    
    train_model_task = PythonOperator(
        task_id="model_training",
        python_callable=train_model
    )

    load_data_task >> validate_schema_task >> feature_engineering_task >> train_model_task