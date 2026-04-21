# Pipelines

This package contains the data movement and transformation layer for CryptoQuant. It covers ingestion, schema enforcement, Delta Lake storage, medallion transforms, orchestration, and validation.

## Package map

- [ingestion/](ingestion/) - batch and streaming market ingestion.
- [transformers/](transformers/) - raw, Bronze, Silver, and Gold DataFrame transforms.
- [schema/](schema/) - Spark schema definitions for each layer.
- [storage/](storage/) - Delta Lake readers and writers.
- [orchestration/](orchestration/) - batch pipeline entry points.
- [utils/](utils/) - shared Spark, config, and logging helpers.
- [validation/](validation/) - data quality checks.

## Current flow

1. Batch backfill or the streaming WebSocket path produces raw market rows.
2. Sentiment jobs under `ingestion/streaming/sentiment/` pull reddit, youtube, and news events and publish to Kafka topics.
2. Raw data is normalized into Bronze.
3. Bronze is cleaned into Silver.
4. Silver is transformed into Gold features.
5. Delta writers persist each layer with the configured partitioning and checkpoint locations.

## Future direction

- Keep batch and streaming implementations aligned around the same schemas and transforms.
- Add more automated validation and operational metadata as the platform grows.

## Sentiment Producers

- Run all sentiment producers with one command:
	- `python -m pipelines.ingestion.streaming.sentiment.run_all`
- Optional polling interval overrides:
	- `python -m pipelines.ingestion.streaming.sentiment.run_all --reddit-interval 300 --youtube-interval 180 --news-interval 120`
- Source layout:
	- `pipelines/ingestion/streaming/sources/sentiment/shared/` - event normalization and shared helpers.
	- `pipelines/ingestion/streaming/sources/sentiment/reddit/` - Reddit config and fetch logic.
	- `pipelines/ingestion/streaming/sources/sentiment/youtube/` - YouTube config and fetch logic.
	- `pipelines/ingestion/streaming/sources/sentiment/news/` - RSS and CryptoPanic fetch logic.

## Ingestion Metrics

- Show per-minute ingested rows for Bronze, Silver, and Gold Delta tables (default last 60 minutes):
	- `python -m pipelines.scripts.per_minute_ingested_data`
- Customize lookback window (for example, last 180 minutes):
	- `python -m pipelines.scripts.per_minute_ingested_data --minutes 180`