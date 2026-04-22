from models.inference.pipeline import InferencePipeline
from models.registry.model_loader import load_model
from utils_global.config_loader import load_config

model_config = load_config("configs/model.yaml")

class RealtimePredictor:
    def __init__(self):
        self.model = load_model()
        self.pipeline = InferencePipeline(self.model, model_config)

    def predict(self, df):
        return self.pipeline.run(df)