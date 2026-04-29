# CryptoQuant

CryptoQuant is a crypto market MLOps workspace for Binance market data. The project ingests raw OHLCV candles, writes them into a Delta Lake bronze/silver/gold layout, engineers features for training and inference, serves predictions through FastAPI, and tracks model health through lightweight drift monitoring.

## Project Layout

- [api/](api/) - FastAPI prediction service and request schemas.
- [airflow/](airflow/) - DAGs for batch data, streaming sync, batch predictions, model training, and drift monitoring.
- [dashboard/](dashboard/) - Streamlit monitoring and research dashboard.
- [docker/](docker/) - Container images for the API, Airflow, Spark, Kafka, and stream producer.
- [models/](models/) - data loading, feature engineering, training, evaluation, inference, and registry helpers.
- [pipelines/](pipelines/) - Medallion ETL jobs, streaming ingestion, schemas, storage, and validation.
- [configs/](configs/) - YAML configuration for data, Kafka, Spark, and model settings.
- [delta/](delta/) - Local Delta Lake storage for raw_data, bronze, silver, gold, predictions, and state.
- [scripts/](scripts/) - thin launch scripts for local development.
- [docs/](docs/) - architecture, commands, and data documentation.

## Runtime Flow

1. Batch ingestion jobs fetch historical Binance data and normalize it into the raw and bronze layers.
2. Streaming ingestion publishes live market data into Kafka, then Spark Structured Streaming writes it into the same Delta contracts.
3. Transformation jobs clean, enrich, and promote the data into silver and gold tables.
4. Training jobs consume the gold feature contract to build and save model artifacts.
5. Prediction jobs and the FastAPI service reuse the saved artifacts for online and batch scoring.
6. Airflow coordinates batch data, batch predictions, training, and drift monitoring workflows.
7. The dashboard surfaces drift history, model metrics from MLflow, and retraining state.

## Local Stack

The canonical local setup is the compose stack defined in [docker-compose.yml](docker-compose.yml). It brings up Kafka, topic initialization, the stream producer, Spark streaming and prediction jobs, FastAPI, Streamlit, Airflow, PostgreSQL, and MLflow.

Start the stack with:

```bash
docker compose up
```

Useful entry points:

```bash
./scripts/run_api.sh
streamlit run dashboard/app.py
```

For operational commands, resets, and service-specific checks, use [docs/commands.md](docs/commands.md).

## Documentation

- [Architecture](docs/architecture.md)
- [Commands](docs/commands.md)
- [API](api/README.md)
- [Airflow](airflow/README.md)
- [Dashboard](dashboard/README.md)
- [Pipelines](pipelines/README.md)
- [Models](models/README.md)
- [Configs](configs/README.md)
- [Scripts](scripts/README.md)

## Design Goals

- Keep batch and streaming paths aligned around the same Bronze/Silver/Gold contracts.
- Treat feature generation as the reusable boundary between training and serving.
- Keep orchestration, observability, and serving concerns separated so the stack can grow without coupling the runtime paths.
