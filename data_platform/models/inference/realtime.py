from models.inference.pipeline import InferencePipeline
from models.registry.model_loader import load_model
from utils_global.config_loader import load_config
from utils_global.logger import get_logger

model_config = load_config("configs/model.yaml")
data_config = load_config("configs/data.yaml")

logger = get_logger("RealtimePredictor")

class RealtimePredictor:
    def __init__(self):
        interval = data_config.get("interval", "unknown")
        tasks = model_config.get("models", {})
        algos = list((model_config.get("model_params") or {}).keys()) or ["xgboost", "lightgbm", "catboost"]

        if not tasks:
            self.models = {"prediction": load_model(logger)}
            self.pipeline = InferencePipeline(self.models, model_config)
            return

        self.models = {}
        for task_name in tasks.keys():
            loaded = None
            for algo in algos:
                model_name = f"cryptoquant.{task_name}.{interval}.{algo}"
                try:
                    loaded = load_model(logger, model_name=model_name)
                    logger.info("Loaded inference model '%s' for task '%s'", model_name, task_name)
                    break
                except Exception as exc:
                    logger.warning("Failed loading model '%s': %s", model_name, exc)
                    continue
            if loaded is None:
                raise RuntimeError(f"No registered model could be loaded for task '{task_name}'")
            self.models[task_name] = loaded

        self.pipeline = InferencePipeline(self.models, model_config)

    def predict(self, df):
        return self.pipeline.run(df)