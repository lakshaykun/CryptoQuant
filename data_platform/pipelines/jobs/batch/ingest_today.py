# pipelines/jobs/batch/ingest_today.py

from pipelines.ingestion.batch.jobs.market.fetch_today import fetch_market_today
from utils_global.config_loader import load_config
from utils_global.logger import get_logger


def main():
    logger = get_logger("ingest_today_job")

    config = load_config("configs/data.yaml")

    symbols = config.get("symbols") or []
    interval = config.get("interval")
    base_path = config.get("raw_data_path", {}).get("market")

    fetch_market_today(
        symbols=symbols,
        interval=interval,
        logger=logger,
        base_path=base_path,
    )

    logger.info("Today ingestion complete")


if __name__ == "__main__":
    main()