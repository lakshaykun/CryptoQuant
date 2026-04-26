# models/data/schema.py

import pandas as pd
from utils_global.logger import get_logger
from utils_global.config_loader import load_config

logger = get_logger(__name__)
config = load_config("configs/model.yaml")

def validate_schema():
    df = pd.read_parquet(config["train_data_path"])
    logger.info("Data loaded for schema validation")

    expected_columns = config["expected_columns"]

    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    
    logger.info("Schema validation passed")