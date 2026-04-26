# pipelines/storage/local/csv.py

import os
import pandas as pd
import glob


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def build_filename(symbol: str, interval: str, date_str: str) -> str:
    return f"{symbol}_{interval}_{date_str}.csv"


def build_path(base_path: str, filename: str) -> str:
    return os.path.join(base_path, filename)


def file_exists(path: str) -> bool:
    return os.path.exists(path)


def save_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)

def read_csv(spark, path, schema):
    df = spark.read.csv(
            path = path,
            schema = schema,
            header = True,
        )
    
    return df


def clean_raw_csv(base_path: str, logger):
    pattern = os.path.join(base_path, "*.csv")
    files = glob.glob(pattern)

    if not files:
        logger.info("No raw files to delete")
        return

    for file in files:
        try:
            os.remove(file)
            logger.info(f"Deleted raw file: {file}")
        except Exception as e:
            logger.error(f"Failed to delete {file}: {e}")