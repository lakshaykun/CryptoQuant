import mlflow
from utils_global.config_loader import load_config
import mlflow.sklearn
import os
import tempfile
import pandas as pd

mlflow_config = load_config("configs/mlflow.yaml")

mlflow.set_tracking_uri(mlflow_config["tracking_uri"])
mlflow.set_experiment(mlflow_config["experiment_name"])
mlflow.set_registry_uri(mlflow_config["tracking_uri"])


def start_run(run_name=None, run_id=None):
    return mlflow.start_run(run_name=run_name, run_id=run_id)


def log_params(params: dict):
    for k, v in params.items():
        mlflow.log_param(k, v)


def log_metrics(metrics: dict):
    for k, v in metrics.items():
        try:
            mlflow.log_metric(k, float(v))
        except (TypeError, ValueError):
            continue


def log_model(model, model_name, feature_version):
    mlflow.sklearn.log_model(model, f"model_{model_name}")
    mlflow.set_tag("model_name", model_name)
    mlflow.set_tag("feature_version", feature_version)

def log_artifact(file_path, artifact_path=None):
    mlflow.log_artifact(file_path, artifact_path)


def log_dataframe(df: pd.DataFrame, artifact_name: str, artifact_path=None):
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    temp_file.close()

    try:
        df.to_csv(temp_file.name, index=False)
        target_path = artifact_path or "evaluation"
        log_artifact(temp_file.name, os.path.join(target_path, artifact_name))
    finally:
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)

def log_scaler(scaler):
    mlflow.sklearn.log_model(scaler, f"scaler")

def register_model(run_id, model_name, registered_model_name=None):
    if not run_id or not model_name:
        raise ValueError("Cannot register model without a successful run and model name")

    model_uri = f"runs:/{run_id}/model_{model_name}"
    final_name = registered_model_name or mlflow_config["model_name"]
    return mlflow.register_model(
        model_uri,
        final_name
    )