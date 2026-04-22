import mlflow
import os
from functools import lru_cache
from mlflow.tracking import MlflowClient
from utils_global.config_loader import load_config

mlflow_config = load_config("configs/mlflow.yaml")
mlflow_tracking_uri = os.getenv(
    "MLFLOW_TRACKING_URI",
    mlflow_config["local_tracking_uri"],
)

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

        version_candidates = sorted(
            latest_versions,
            key=lambda version: int(version.version),
            reverse=True,
        )

        load_errors = []

        for version in version_candidates:
            try:
                if hasattr(client, "get_model_version_download_uri"):
                    model_uri = client.get_model_version_download_uri(
                        registered_model_name,
                        version.version,
                    )
                else:
                    model_uri = f"models:/{registered_model_name}/{version.version}"

                return mlflow.pyfunc.load_model(model_uri)
            except Exception as exc:
                load_errors.append(
                    f"version {version.version} from {version.source}: {exc}"
                )

        raise RuntimeError(
            "No registered MLflow model versions could be loaded for "
            f"'{registered_model_name}' from '{mlflow_tracking_uri}'. "
            f"Tried: {'; '.join(load_errors)}"
        )

    except Exception as exc:
        raise RuntimeError(
            f"Failed to load MLflow model '{registered_model_name}' from '{mlflow_tracking_uri}': {exc}"
        ) from exc