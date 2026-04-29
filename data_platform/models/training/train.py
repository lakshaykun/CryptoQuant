# models/training/train.py

from models.training.trainer import Trainer
from utils_global.logger import get_logger
from utils_global.config_loader import load_config
import pandas as pd

logger = get_logger(__name__)
config = load_config("configs/model.yaml")
data_config = load_config("configs/data.yaml")


def train_model():
    config["data_interval"] = data_config.get("interval", "unknown")
    df = pd.read_parquet(config["train_data_path"])

    logger.info("Data loaded for model training")

    trainer = Trainer(config, logger)
    trainer.train(df)

    logger.info(f"Models trained and registered successfully")
