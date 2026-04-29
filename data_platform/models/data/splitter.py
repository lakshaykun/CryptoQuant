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
    val_end = test_start - pd.Timedelta(days=1)
    val_start = val_end - pd.Timedelta(days=val_days)
    train_end = val_start - pd.Timedelta(days=1)
    train_start = train_end - pd.Timedelta(days=train_days)

    # =========================
    # SPLIT
    # =========================
    train = df[(df["open_time"] >= train_start) & (df["open_time"] < train_end)]
    val = df[(df["open_time"] >= val_start) & (df["open_time"] < val_end)]
    test = df[df["open_time"] >= test_start]

    return train, val, test


def resolve_model_split_config(model_cfg: dict, global_cfg: dict) -> dict:
    model_split = (model_cfg or {}).get("time_split_days") or {}
    global_split = (global_cfg or {}).get("time_split_days") or {}
    resolved = {
        "train": int(model_split.get("train", global_split.get("train", 90))),
        "val": int(model_split.get("val", global_split.get("val", 14))),
        "test": int(model_split.get("test", global_split.get("test", 7))),
    }
    return resolved


def split_for_model(df: DataFrame, split_cfg: dict) -> tuple[DataFrame, DataFrame, DataFrame, dict]:
    ordered = df.sort_values(["symbol", "open_time"]).reset_index(drop=True)
    max_time = ordered["open_time"].max()

    test_start = max_time - pd.Timedelta(days=split_cfg["test"])
    val_end = test_start - pd.Timedelta(days=1)
    val_start = val_end - pd.Timedelta(days=split_cfg["val"])
    train_end = val_start - pd.Timedelta(days=1)
    train_start = train_end - pd.Timedelta(days=split_cfg["train"])

    train = ordered[(ordered["open_time"] >= train_start) & (ordered["open_time"] < train_end)]
    val = ordered[(ordered["open_time"] >= val_start) & (ordered["open_time"] < val_end)]
    test = ordered[ordered["open_time"] >= test_start]

    metadata = {
        "train_start": train_start,
        "val_start": val_start,
        "test_start": test_start,
        "max_time": max_time,
        "train_rows": len(train),
        "val_rows": len(val),
        "test_rows": len(test),
    }
    return train, val, test, metadata