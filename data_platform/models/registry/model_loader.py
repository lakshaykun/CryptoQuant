import mlflow
from functools import lru_cache
from mlflow.tracking import MlflowClient
from utils_global.config_loader import load_config

mlflow_config = load_config("configs/mlflow.yaml")
mlflow_tracking_uri = mlflow_config["local_tracking_uri"]

mlflow.set_tracking_uri(mlflow_tracking_uri)
mlflow.set_registry_uri(mlflow_tracking_uri)

@lru_cache(maxsize=1)
def load_model(model_name=None):
    """
    Load the latest registered MLflow model version.

    Args:
        model_name (str | None): The registered model name to load.
    Returns:
        The loaded model.
    """
    registered_model_name = model_name or mlflow_config["model_name"]

    try:
        client = MlflowClient(tracking_uri=mlflow_tracking_uri)
        latest_versions = client.search_model_versions(f"name='{registered_model_name}'")

        if not latest_versions:
            raise ValueError(
                f"No registered versions found for model '{registered_model_name}'"
            )

        latest_version = max(latest_versions, key=lambda version: int(version.version))
        model_uri = f"models:/{registered_model_name}/{latest_version.version}"
        return mlflow.pyfunc.load_model(model_uri)

    except Exception as exc:
        raise RuntimeError(
            f"Failed to load MLflow model '{registered_model_name}' from '{mlflow_tracking_uri}': {exc}"
        ) from exc