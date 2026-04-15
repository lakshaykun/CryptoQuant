import joblib

def load_local_model(model_path: str):
    return joblib.load(model_path)