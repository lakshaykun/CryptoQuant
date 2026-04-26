# models/data/splitter.py

from pandas import DataFrame

def time_split(df: DataFrame, test_ratio: float=0.2) -> tuple[DataFrame, DataFrame]:
    df = df.sort_values("open_time")

    split_index = int(len(df) * (1 - test_ratio))
    
    train = df.iloc[:split_index]
    test = df.iloc[split_index:]

    return train, test