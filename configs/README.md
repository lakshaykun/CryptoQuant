# Configs

YAML configuration lives here so runtime behavior can be changed without editing code.

## Files

- `data.yaml` - symbols, interval, historical backfill start date, Delta table paths, and the upstream WebSocket URI.
- `spark.yaml` - Spark application name, local master, Delta support, AQE, and performance settings.
- `kafka.yaml` - broker aliases and topic settings for streaming ingestion.

## Usage pattern

The pipeline code loads these files directly through `pipelines.utils.config_loader.load_config`, so updates here affect batch, streaming, and serving flows consistently.

## Future direction

- Add environment-specific overlays for local, staging, and production runs.
- Move toward schema-validated config objects as the project grows.