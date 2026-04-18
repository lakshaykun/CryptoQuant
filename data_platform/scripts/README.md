# Scripts

Operational entry points for local development live here.

## Files

- `run_batch.py` - launches the batch pipeline.
- `run_api.sh` - activates the local environment when available and starts the FastAPI app.
- `export_gold_parquet_to_csv.py` - reads parquet data from the Gold layer and exports it as a single CSV file.

## Usage pattern

Use scripts for repeatable local tasks, but keep the actual implementation in the package modules under `api/`, `pipelines/`, and `models/`.

Export Gold parquet to CSV:

```bash
python scripts/export_gold_parquet_to_csv.py --input delta/gold --output delta/gold.csv
```

## Future direction

- Add scripts for streaming jobs, model training, and validation runs.
- Keep the scripts thin so they remain easy to audit and maintain.