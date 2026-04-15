# models/training/train.py

import joblib
from models.data.splitter import time_split
from models.training.trainer import Trainer
from models.evaluation.evaluate import evaluate_model
from utils.logger import get_logger
from utils.config_loader import load_config
import pandas as pd

logger = get_logger(__name__)
config = load_config("configs/model.yaml")


def train_model():
    df = pd.read_parquet(config["train_data_path"])

    logger.info("Data loaded for model training")

    df = df.dropna()

    train_df, test_df = time_split(df)

    trainer = Trainer(config, logger)
    model = trainer.train(train_df)

    evaluate_model(model, test_df)

    joblib.dump(model, config["model_path"])

    logger.info(f"Model trained and saved to {config['model_path']}")

if __name__ == "__main__":
    train_model()