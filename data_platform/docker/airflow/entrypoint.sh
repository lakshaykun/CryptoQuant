#!/bin/bash

set -e

echo "Starting Airflow..."

# Always run migrate (safe for Postgres)
airflow db migrate

echo "Creating Airflow user (if not exists)..."

airflow users list | grep airflow > /dev/null 2>&1 || airflow users create \
    --username airflow \
    --password airflow \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com

echo "Starting service..."

exec airflow "$@"