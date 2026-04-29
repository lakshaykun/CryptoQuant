import numpy as np
import pandas as pd


def backtest(y_test, preds, threshold=0.0, transaction_cost=0.0, direct_signal=False):
    df = pd.DataFrame({"actual_return": y_test, "prediction": preds})

    if direct_signal:
        df["signal"] = np.clip(np.rint(df["prediction"]).astype(int), -1, 1)
    else:
        df["signal"] = np.select(
            [df["prediction"] > threshold, df["prediction"] < -threshold],
            [1, -1],
            default=0,
        )

    df["turnover"] = df["signal"].diff().abs().fillna(0)
    df["strategy_return"] = df["signal"] * df["actual_return"]
    if transaction_cost:
        df["strategy_return"] = df["strategy_return"] - (df["turnover"] * transaction_cost)

    df["benchmark_return"] = df["actual_return"]
    df["strategy_cumulative_return"] = (1 + df["strategy_return"]).cumprod()
    df["benchmark_cumulative_return"] = (1 + df["benchmark_return"]).cumprod()

    df["strategy_running_max"] = df["strategy_cumulative_return"].cummax()
    df["strategy_drawdown"] = (
        df["strategy_cumulative_return"] / df["strategy_running_max"]
    ) - 1

    df["benchmark_running_max"] = df["benchmark_cumulative_return"].cummax()
    df["benchmark_drawdown"] = (
        df["benchmark_cumulative_return"] / df["benchmark_running_max"]
    ) - 1

    return df