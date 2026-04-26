# pipelines/jobs/batch/cleanup_raw.py

from pipelines.storage.local.csv import clean_raw_csv
from utils_global.config_loader import load_config
from utils_global.logger import get_logger


def cleanup_task():
    logger = get_logger("cleanup_job")
    config = load_config("configs/data.yaml")
    raw_path = config.get("raw_data_path", {}).get("market", "/opt/app/raw_data/market/")

    clean_raw_csv(raw_path, logger)

if __name__ == "__main__":
    cleanup_task()