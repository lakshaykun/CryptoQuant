from models.inference.pipeline import InferencePipeline
from models.registry.model_loader import load_local_model

class RealtimePredictor:
    def __init__(self):
        self.model = load_local_model()
        self.pipeline = InferencePipeline(self.model)

    def predict(self, df):
        return self.pipeline.run(df)
    
    def predictBase(self, df):
        return self.pipeline.runBase(df)