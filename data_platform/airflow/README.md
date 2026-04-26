# Airflow

This folder contains production DAGs for scheduled data movement.

## Available DAGs

- `batch_data_pipeline`
	- Market batch flow: ingest -> bronze -> silver -> gold -> cleanup.
- `sentiment_streaming_pipeline`
	- Runs every 1 minute with graph:
	- `preflight_streaming -> (ingest_reddit_streaming || ingest_youtube_streaming || ingest_news_streaming || ingest_telegram_streaming) -> bronze_delta_lake_streaming -> silver_delta_lake_streaming -> gold_delta_lake_streaming`
	- Each run ingests only the last `sentiment.streaming_lookback_minutes` from `configs/data.yaml`.
- `sentiment_batch_pipeline`
	- Manual-trigger DAG with graph:
	- `preflight_batch -> (ingest_reddit_batch || ingest_youtube_batch || ingest_news_batch || ingest_telegram_batch) -> bronze_delta_lake_batch -> silver_delta_lake_batch -> gold_delta_lake_batch`
	- Uses state-aware catch-up mode (`--mode batch`).

## Run Steps

- Rebuild images after code/config changes:
	- `docker compose build --no-cache spark airflow-webserver airflow-scheduler model_server`
- Start all required services:
	- `docker compose up -d kafka topic-init spark model_server postgres airflow-webserver airflow-scheduler`
- Verify containers are healthy:
	- `docker ps`
	- `docker logs airflow-webserver --tail 100`
	- `docker logs airflow-scheduler --tail 100`
	- `docker logs model-server --tail 100`
- Open Airflow UI:
	- `http://localhost:8080` (user/password: `airflow` / `airflow`)
- Unpause DAGs:
	- `sentiment_batch_pipeline`
	- `sentiment_streaming_pipeline`
- Trigger batch backfill DAG:
	- `docker exec airflow-webserver airflow dags trigger sentiment_batch_pipeline`
- Trigger/verify streaming DAG:
	- `docker exec airflow-webserver airflow dags trigger sentiment_streaming_pipeline`
	- Or leave unpaused to run every minute.
- Verify Delta medallion output:
	- `docker exec spark python - <<'PY'`
	- `from pyspark.sql import SparkSession`
	- `spark = SparkSession.builder.getOrCreate()`
	- `for n,p in {'bronze':'/opt/app/delta/bronze/sentiment','silver':'/opt/app/delta/silver/sentiment','gold':'/opt/app/delta/gold/sentiment'}.items():`
	- `    print(n, spark.read.format('delta').load(p).count())`
	- `PY`
- If `preflight_*` fails, open the task log in Airflow; it prints exactly which source/service is blocked (missing creds or network/DNS issue).
