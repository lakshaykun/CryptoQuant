import numpy as np
from models.evaluation.metrics import rmse
from models.evaluation.backtesting import backtest
from utils.config_loader import load_config


def evaluate_model(model, df):
    df = df.copy()
    model_config = load_config("configs/model.yaml")
    X = df[model_config["features"]]
    y = df[model_config["target"]]

    preds = model.predict(X)

    print("RMSE:", rmse(y, preds))

    # Directional accuracy (VERY IMPORTANT)
    direction_acc = np.mean((preds > 0) == (y > 0))
    print("Directional Accuracy:", direction_acc)

    df["prediction"] = preds

    bt = backtest(df)

    print("Final Return:", bt["cumulative_return"].iloc[-1])