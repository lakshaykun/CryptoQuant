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
2. Raw data is normalized into Bronze.
3. Bronze is cleaned into Silver.
4. Silver is transformed into Gold features.
5. Delta writers persist each layer with the configured partitioning and checkpoint locations.

## Future direction

- Keep batch and streaming implementations aligned around the same schemas and transforms.
- Add more automated validation and operational metadata as the platform grows.