# models/training/train.py

from models.data.splitter import time_split
from models.training.trainer import Trainer
from utils_global.logger import get_logger
from utils_global.config_loader import load_config
import pandas as pd

logger = get_logger(__name__)
config = load_config("configs/model.yaml")


def train_model():
    df = pd.read_parquet(config["train_data_path"])

    logger.info("Data loaded for model training")

    df = df.dropna()

    train_df, val_df, test_df = time_split(
                                    df, 
                                    config["time_split_days"]['train'], 
                                    config["time_split_days"]['val'], 
                                    config["time_split_days"]['test']
                                )

    trainer = Trainer(config, logger)
    trainer.train(train_df, val_df, test_df)

    logger.info(f"Models trained and registered successfully")
