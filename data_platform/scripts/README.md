# Scripts

Operational entry points for local development live here.

## Files

- `run_batch.py` - launches the batch pipeline.
- `run_api.sh` - activates the local environment when available and starts the FastAPI app.

## Usage pattern

Use scripts for repeatable local tasks, but keep the actual implementation in the package modules under `api/`, `pipelines/`, and `models/`.

## Future direction

- Add scripts for streaming jobs, model training, and validation runs.
- Keep the scripts thin so they remain easy to audit and maintain.