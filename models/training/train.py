import joblib
from models.data.loader import load_silver_data
from models.data.schema import validate_schema
from models.data.splitter import time_split
from models.training.trainer import Trainer
from models.evaluation.evaluate import evaluate_model

def train_pipeline():
    df = load_silver_data()

    validate_schema(df)
    df = df.dropna()
    train_df, test_df = time_split(df)

    trainer = Trainer()
    model = trainer.train(train_df)

    evaluate_model(model, test_df)

    joblib.dump(model, "models/artifacts/model.pkl")

    print("✅ Training complete and model saved")

if __name__ == "__main__":
    train_pipeline()