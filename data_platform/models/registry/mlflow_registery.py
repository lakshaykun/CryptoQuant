import mlflow
import mlflow.sklearn

def log_model(model):
    with mlflow.start_run():
        mlflow.sklearn.log_model(model, "model")