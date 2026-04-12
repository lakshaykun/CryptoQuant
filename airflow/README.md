# Airflow

This directory is reserved for workflow orchestration. It is currently a placeholder, but it should eventually hold DAGs for batch backfills, scheduled retraining, data quality checks, and model promotion.

## Planned responsibilities

- Schedule historical backfills into Bronze, Silver, and Gold.
- Trigger retraining when new data or drift signals arrive.
- Coordinate validation, evaluation, and registration steps.
- Provide a single place for operational lineage across the ML lifecycle.

## Future direction

As the project matures, this folder should host reusable DAG components instead of ad hoc scripts so that ingestion, training, and deployment can be managed together.