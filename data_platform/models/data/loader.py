# models/data/loader.py

from deltalake import DeltaTable
from utils.logger import get_logger
from utils.config_loader import load_config

logger = get_logger(__name__)
data_config = load_config("configs/data.yaml")

def load_data():
    dt = DeltaTable(data_config["tables"]["gold_market"]["path"])
    model_config = load_config("configs/model.yaml")
    
    df = dt.to_pandas(
        columns=model_config["expected_columns"]
    )
    
    path = model_config["train_data_path"]

    df.to_parquet(path)

    logger.info(f"Data saved to {path}")

if __name__ == "__main__":
    load_data()