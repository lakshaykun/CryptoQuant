from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib


ARTIFACT_DIR = Path("models/artifacts")


def _ensure_artifact_dir() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)


def save_model(model: Any, model_name: str) -> str:
    _ensure_artifact_dir()
    model_path = ARTIFACT_DIR / "best_model.pkl"
    joblib.dump(model, model_path)
    meta_path = ARTIFACT_DIR / "model_meta.json"
    meta_path.write_text(json.dumps({"model_name": model_name}, indent=2), encoding="utf-8")
    return str(model_path)


def save_scaler(scaler: Any) -> str:
    _ensure_artifact_dir()
    scaler_path = ARTIFACT_DIR / "scaler.pkl"
    joblib.dump(scaler, scaler_path)
    return str(scaler_path)


def save_metrics(metrics: dict[str, Any], leaderboard: list[dict[str, Any]]) -> None:
    _ensure_artifact_dir()
    (ARTIFACT_DIR / "best_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    (ARTIFACT_DIR / "leaderboard.json").write_text(json.dumps(leaderboard, indent=2), encoding="utf-8")
