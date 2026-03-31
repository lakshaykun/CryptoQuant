import joblib
from models.inference.pipeline import InferencePipeline

class RealtimePredictor:
    def __init__(self):
        self.model = joblib.load("models/artifacts/model.pkl")
        self.pipeline = InferencePipeline(self.model)

    def predict(self, df):
        return self.pipeline.run(df)