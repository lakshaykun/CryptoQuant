from xgboost import XGBRegressor
from models.config.model_config import FEATURE_COLUMNS, TARGET_COLUMN, MODEL_PARAMS

class Trainer:
    def train(self, df):
        df = df.copy()

        X = df[FEATURE_COLUMNS]
        y = df[TARGET_COLUMN]

        model = XGBRegressor(**MODEL_PARAMS)
        model.fit(X, y)

        return model