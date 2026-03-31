def time_split(df, test_ratio=0.2):
    df = df.sort_values("timestamp")

    split_index = int(len(df) * (1 - test_ratio))
    
    train = df.iloc[:split_index]
    test = df.iloc[split_index:]

    return train, test