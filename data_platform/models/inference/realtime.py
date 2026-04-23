from models.inference.pipeline import InferencePipeline
from models.registry.model_loader import load_model
from utils_global.config_loader import load_config
from utils_global.logger import get_logger

model_config = load_config("configs/model.yaml")

logger = get_logger("RealtimePredictor")

class RealtimePredictor:
    def __init__(self):
        self.model = load_model(logger)
        self.pipeline = InferencePipeline(self.model, model_config)

    def predict(self, df):
        return self.pipeline.run(df)