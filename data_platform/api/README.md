# API

FastAPI service for online predictions. The API loads the local model artifact, converts incoming payloads into pandas DataFrames, and returns JSON-friendly prediction arrays.

## Endpoints

- `GET /` - health check.
- `POST /predict` - accepts engineered feature rows and returns task-wise prediction values under `predictions`.
- `GET /drift` - returns the latest drift summary and top drifting features from Delta drift history.

## Key files

- `app.py` - FastAPI app and route handlers.
- `schemas/request.py` - request models for raw and engineered payloads.
- `schemas/model.py` - shared schema definitions for market rows.

## Contract notes

- Keep the engineered prediction route aligned with the feature columns in `configs/model.yaml`.
- Treat DataFrame conversion as the boundary between HTTP payloads and model inference.
- Keep drift summary output aligned with rows persisted by `models/monitoring/drift.py`.

## Future work

- Add response schemas and versioned request models.
- Add auth, rate limiting, and request logging when the service is exposed publicly.
- Return richer metadata such as model version, latency, and feature validation results.