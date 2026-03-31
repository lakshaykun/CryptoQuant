import joblib

def load_local_model():
    return joblib.load("models/artifacts/model.pkl")