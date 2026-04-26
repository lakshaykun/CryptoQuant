import pandas as pd

from utils_global.config_loader import load_config


data_config = load_config("configs/data.yaml")


class InferencePipeline:
    def __init__(self, model, model_config):
        self.model = model
        self.model_config = model_config

    def _prepare_features(self, df):
        X = df[self.model_config.get("features")].copy()

        if "symbol" in X.columns and not pd.api.types.is_numeric_dtype(X["symbol"]):
            symbols = data_config.get("symbols") or []
            symbol_map = {symbol: index for index, symbol in enumerate(symbols)}
            X["symbol"] = X["symbol"].map(symbol_map).fillna(-1).astype(int)

        return X

    def run(self, df):
        X = self._prepare_features(df)
        if X.empty:
            raise ValueError("No engineered feature rows provided")
        return self.model.predict(X)
