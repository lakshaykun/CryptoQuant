import numpy as np
from models.config.model_config import FEATURE_COLUMNS, TARGET_COLUMN
from models.evaluation.metrics import rmse
from models.evaluation.backtesting import backtest

def evaluate_model(model, df):
    df = df.copy()

    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]

    preds = model.predict(X)

    print("RMSE:", rmse(y, preds))

    # Directional accuracy (VERY IMPORTANT)
    direction_acc = np.mean((preds > 0) == (y > 0))
    print("Directional Accuracy:", direction_acc)

    df["prediction"] = preds

    bt = backtest(df)

    print("Final Return:", bt["cumulative_return"].iloc[-1])