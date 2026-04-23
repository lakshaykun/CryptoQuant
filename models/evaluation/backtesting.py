import numpy as np
import pandas as pd

def backtest(X_test, y_test, preds):
    df = pd.DataFrame({"log_return": y_test, "prediction": preds})

    # Signal: long if positive prediction
    df["signal"] = np.where(df["prediction"] > 0, 1, -1)

    # Strategy return
    df["strategy_return"] = df["signal"] * df["log_return"]

    # Cumulative return
    df["cumulative_return"] = (1 + df["strategy_return"]).cumprod()

    return df