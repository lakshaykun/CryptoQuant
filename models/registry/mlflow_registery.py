import mlflow
from utils_global.config_loader import load_config
import mlflow.sklearn

mlflow_config = load_config("configs/mlflow.yaml")

mlflow.set_tracking_uri(mlflow_config["tracking_uri"])
mlflow.set_experiment(mlflow_config["experiment_name"])


def start_run(run_name=None):
    return mlflow.start_run(run_name=run_name)


def log_params(params: dict):
    for k, v in params.items():
        mlflow.log_param(k, v)


def log_metrics(metrics: dict):
    for k, v in metrics.items():
        mlflow.log_metric(k, v)


def log_model(model, model_name, feature_version):
    mlflow.sklearn.log_model(model, f"model_{model_name}")
    mlflow.set_tag("model_name", model_name)
    mlflow.set_tag("feature_version", feature_version)

def log_artifact(file_path, artifact_path=None):
    mlflow.log_artifact(file_path, artifact_path)

def log_scaler(scaler):
    mlflow.sklearn.log_model(scaler, f"scaler")

def register_model(run_id, model_name):
    model_uri = f"runs:/{run_id}/model_{model_name}"
    return mlflow.register_model(
        model_uri,
        mlflow_config["model_name"]
    )