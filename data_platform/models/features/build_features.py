import numpy as np
import pandas as pd


def sanitize_features(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    clean.replace([np.inf, -np.inf], np.nan, inplace=True)
    clean.fillna(0, inplace=True)

    numeric_cols = clean.select_dtypes(include=[np.number]).columns
    zero_var_cols = [col for col in numeric_cols if float(clean[col].std(ddof=0)) == 0.0]
    if zero_var_cols:
        clean = clean.drop(columns=zero_var_cols)

    return clean
