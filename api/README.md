# API

FastAPI service for online predictions. The API loads the local model artifact, converts incoming payloads into pandas DataFrames, and returns JSON-friendly prediction arrays.

## Endpoints

- `GET /` - health check.
- `POST /predict/base` - accepts raw OHLCV rows and builds features internally before scoring.
- `POST /predict/engineered` - accepts precomputed feature rows and scores them directly.

## Key files

- `app.py` - FastAPI app and route handlers.
- `schemas/request.py` - request models for raw and engineered payloads.
- `schemas/model.py` - shared schema definitions for market rows.

## Contract notes

- Keep the raw prediction route aligned with the Bronze OHLCV schema.
- Keep the engineered route aligned with the feature columns in `models/config/model_config.py`.
- Treat DataFrame conversion as the boundary between HTTP payloads and model inference.

## Future work

- Add response schemas and versioned request models.
- Add auth, rate limiting, and request logging when the service is exposed publicly.
- Return richer metadata such as model version, latency, and feature validation results.