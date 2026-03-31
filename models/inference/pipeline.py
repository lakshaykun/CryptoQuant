from models.config.model_config import FEATURE_COLUMNS

class InferencePipeline:
    def __init__(self, model):
        self.model = model

    def run(self, df):
        X = df[FEATURE_COLUMNS]
        return self.model.predict(X)