# models/data/loader.py

from deltalake import DeltaTable
from utils_global.logger import get_logger
from utils_global.config_loader import load_config
from models.features.build_features import sanitize_features

logger = get_logger(__name__)
data_config = load_config("configs/data.yaml")

def load_data():
    dt = DeltaTable(data_config["tables"]["gold_market"]["path"])
    model_config = load_config("configs/model.yaml")
    
    df = dt.to_pandas(
        columns=model_config["expected_columns"]
    )

    df = sanitize_features(df)
    
    path = model_config["train_data_path"]

    df.to_parquet(path)

    logger.info(f"Data saved to {path}")

if __name__ == "__main__":
    load_data()