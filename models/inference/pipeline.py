from models.config.model_config import FEATURE_COLUMNS
from models.features.build_features import build_features

class InferencePipeline:
    def __init__(self, model):
        self.model = model

    def run(self, df):
        X = df[FEATURE_COLUMNS]
        return self.model.predict(X)
    
    def runBase(self, df):
        X = build_features(df)[FEATURE_COLUMNS].values
        return self.model.predict(X)