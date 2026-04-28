# models/data/splitter.py

from pandas import DataFrame
import pandas as pd

def time_split(
    df: DataFrame,
    train_days: int = 90,
    val_days: int = 14,
    test_days: int = 7
) -> tuple[DataFrame, DataFrame, DataFrame]:

    # =========================
    # SORT
    # =========================
    df = df.sort_values(["symbol", "open_time"]).reset_index(drop=True)

    max_time = df["open_time"].max()

    # =========================
    # DEFINE WINDOWS
    # =========================
    test_start = max_time - pd.Timedelta(days=test_days)
    val_start = test_start - pd.Timedelta(days=val_days)
    train_start = val_start - pd.Timedelta(days=train_days)

    # =========================
    # SPLIT
    # =========================
    train = df[(df["open_time"] >= train_start) & (df["open_time"] < val_start)]
    val = df[(df["open_time"] >= val_start) & (df["open_time"] < test_start)]
    test = df[df["open_time"] >= test_start]

    return train, val, test