from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from models.features.feature_engineering import _compute_sign
from models.inference.pipeline import InferencePipeline
from models.utils.horizon import compute_steps_per_day, parse_interval, resolve_horizon_steps


class _DummyModel:
    def __init__(self, values):
        self.values = values

    def predict(self, X):
        if isinstance(self.values, (list, np.ndarray)):
            return np.asarray(self.values)
        return np.asarray([self.values] * len(X))


def test_horizon_interval_logic():
    assert parse_interval("1m") == 1
    assert parse_interval("5m") == 5
    assert parse_interval("1h") == 60
    assert compute_steps_per_day("5m") == 288
    assert resolve_horizon_steps("short", "5m") == 1
    assert resolve_horizon_steps("1d", "15m") == 96


def test_threshold_logic():
    threshold = 0.0002
    assert _compute_sign(0.0003, threshold) == 1
    assert _compute_sign(-0.0003, threshold) == -1
    assert _compute_sign(0.0001, threshold) == 0


def test_inference_feature_alignment_and_output_shape(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    artifacts = Path("models/artifacts")
    artifacts.mkdir(parents=True, exist_ok=True)
    joblib.dump(["f1", "f2"], artifacts / "feature_columns.pkl")

    model_config = {
        "features": ["f1", "f2", "f3"],
        "inference": {"confidence_threshold": 0.2},
    }
    models = {
        "return_short": _DummyModel([0.1, 0.2]),
        "return_long": _DummyModel([0.3, 0.4]),
        "sign_short": _DummyModel([0.1, -0.9]),
        "sign_long": _DummyModel([0.05, 0.8]),
    }
    pipeline = InferencePipeline(models, model_config)
    frame = pd.DataFrame({"f1": [1.0, 2.0], "f3": [3.0, 4.0]})

    output = pipeline.run(frame)
    assert "predictions" in output
    assert set(output["predictions"].keys()) == {"return_short", "return_long", "sign_short", "sign_long"}
    # confidence filter applies to |raw| < 0.2
    assert output["predictions"]["sign_short"][0] == 0
    assert output["predictions"]["sign_short"][1] == -1
    assert output["predictions"]["sign_long"][0] == 0
    assert output["predictions"]["sign_long"][1] == 1


def test_mlflow_naming_contract():
    task = "return_short"
    interval = "5m"
    algo = "xgboost"
    model_name = f"cryptoquant.{task}.{interval}.{algo}"
    assert model_name == "cryptoquant.return_short.5m.xgboost"
