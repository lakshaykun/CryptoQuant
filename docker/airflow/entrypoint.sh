#!/bin/bash

set -e

echo "Initializing Airflow DB..."

if [ ! -f "/opt/airflow/airflow.db" ]; then
    airflow db init
fi

echo "Creating Airflow user..."
airflow users create \
    --username airflow \
    --password airflow \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com || true

echo "Starting Airflow..."

exec airflow "$@"
