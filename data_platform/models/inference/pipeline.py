from models.config.model_config import FEATURE_COLUMNS
from models.features.feature_engineering import build_features

class InferencePipeline:
    def __init__(self, model):
        self.model = model

    def run(self, df):
        X = df[FEATURE_COLUMNS]
        if X.empty:
            raise ValueError("No engineered feature rows provided")
        return self.model.predict(X)
    
    def runBase(self, df):
        X = build_features(df)[FEATURE_COLUMNS]
        if X.empty:
            raise ValueError("Not enough rows to build features")
        return self.model.predict(X)