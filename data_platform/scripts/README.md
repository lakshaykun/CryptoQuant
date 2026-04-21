# Scripts

Operational entry points for local development live here.

## Files

- `run_batch.py` - launches the batch pipeline.
- `run_api.sh` - activates the local environment when available and starts the FastAPI app.
- `export_gold_parquet_to_csv.py` - reads parquet data from the Gold layer and exports it as a single CSV file.
- `export_bronze_silver_gold_parquet_to_csv.py` - exports parquet data from Bronze, Silver, and Gold layers into CSV files.
- `generate_telegram_session.py` - interactive helper that logs in with Telethon and prints TELEGRAM_SESSION_STRING.

## Usage pattern

Use scripts for repeatable local tasks, but keep the actual implementation in the package modules under `api/`, `pipelines/`, and `models/`.

Export Gold parquet to CSV:

```bash
python scripts/export_gold_parquet_to_csv.py --input delta/gold --output delta/gold.csv
```

Export Bronze/Silver/Gold parquet to CSV:

```bash
python scripts/export_bronze_silver_gold_parquet_to_csv.py
python scripts/export_bronze_silver_gold_parquet_to_csv.py --layers bronze,silver,gold --output-dir delta
```

Generate Telegram session string:

```bash
python scripts/generate_telegram_session.py
```

## Future direction

- Add scripts for streaming jobs, model training, and validation runs.
- Keep the scripts thin so they remain easy to audit and maintain.