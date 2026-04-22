# CryptoQuant

CryptoQuant is a crypto market MLOps workspace for Binance market data. It ingests raw OHLCV candles, writes them into a Delta Lake medallion layout, engineers features, trains models, and serves predictions through FastAPI and WebSocket entry points.

## What lives here

- [api/](api/) - FastAPI prediction service.
- [dashboard/](dashboard/) - Streamlit monitoring and quant research dashboard.
- [pipelines/](pipelines/) - batch and streaming ingestion, medallion transforms, Delta I/O, and validation.
- [models/](models/) - feature engineering, training, evaluation, inference, and registry helpers.
- [delta/](delta/) - Delta Lake medallion and state storage used by the pipeline.
- [configs/](configs/) - shared YAML configuration for data, Spark, and Kafka.
- [scripts/](scripts/) - convenience launchers for local development.
- [notebooks/](notebooks/) - exploratory notebooks and prototype analysis.

## Current flow

1. Historical backfill or the Render WebSocket proxy collects Binance candles.
2. [pipelines/ingestion](pipelines/ingestion/) normalizes raw rows into Bronze.
3. [pipelines/transformers](pipelines/transformers/) cleans and standardizes the data into Silver.
4. [pipelines/transformers/gold](pipelines/transformers/gold/) builds model-ready features.
5. [models/](models/) trains, evaluates, and saves local artifacts.
6. [pipelines/jobs/batch](pipelines/jobs/batch/) and [pipelines/jobs/streaming](pipelines/jobs/streaming/) persist model predictions into Delta.
7. [api/](api/) loads the saved model for online prediction.

## Run locally

- Batch pipeline: `python scripts/run_batch.py`
- Prediction API: `./scripts/run_api.sh`
- Streamlit dashboard: `streamlit run dashboard/app.py`

## Documentation

- [Architecture](docs/architecture.md)
- [Prometheus](docs/prometheus.md)
- [Pipelines](pipelines/README.md)
- [API](api/README.md)
- [Models](models/README.md)
- [Dashboard](dashboard/README.md)

## Design goals

- Keep batch and streaming paths aligned around the same Bronze/Silver/Gold contracts.
- Treat feature generation as a reusable boundary between training and serving.
- Leave room for Airflow orchestration, model registry integration, drift monitoring, and multi-exchange expansion.