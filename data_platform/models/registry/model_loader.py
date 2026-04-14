import joblib
from models.config.model_config import MODEL_PATH

def load_local_model():
    return joblib.load(MODEL_PATH)