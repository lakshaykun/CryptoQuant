from xgboost import XGBRegressor
import pandas as pd

class Trainer:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def train(self, df: pd.DataFrame):
        df = df.copy()

        X = df[self.config.get("features")]
        y = df[self.config.get("target")]

        params = self.config.get("model_params", {})
        
        model = XGBRegressor(**params)
        model.fit(X, y)

        return model