#!/bin/bash
set -e

echo "Starting Airflow..."

airflow db migrate

echo "Creating Airflow user (if not exists)..."
airflow users list | grep airflow > /dev/null 2>&1 || airflow users create \
    --username airflow \
    --password airflow \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com

# -----------------------------
# Trigger batch ingestion ONCE
# -----------------------------
if [ "$TRIGGER_INGEST_ON_START" = "true" ]; then
    echo "Waiting for Airflow scheduler..."
    sleep 20

    echo "Checking if DAG already ran..."

    RUN_EXISTS=$(airflow dags list-runs -d batch_data_pipeline | grep running || true)

    if [ -z "$RUN_EXISTS" ]; then
        echo "Unpausing DAG..."
        airflow dags unpause batch_data_pipeline || true

        echo "Triggering batch ingestion DAG..."
        airflow dags trigger batch_data_pipeline || true
    else
        echo "Batch ingestion already running, skipping..."
    fi
fi

echo "Starting service..."
exec airflow "$@"