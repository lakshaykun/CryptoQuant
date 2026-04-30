from __future__ import annotations

import os
from typing import Any

import requests

from utils.env import load_env_file


load_env_file()


DEFAULT_MODEL_ID = "kk08/CryptoBERT"
DEFAULT_TIMEOUT_SECONDS = 30

_SESSION = requests.Session()


def _hf_token() -> str:
    candidates = (
        "HUGGINGFACE_API_KEY",
        "HF_TOKEN",
        "HUGGINGFACEHUB_API_TOKEN",
    )
    for key in candidates:
        value = str(os.getenv(key, "")).strip().strip('"').strip("'")
        if value:
            return value
    return ""


def _model_id() -> str:
    candidate = str(os.getenv("CRYPTOBERT_MODEL_ID", DEFAULT_MODEL_ID)).strip()
    return candidate or DEFAULT_MODEL_ID


def _hf_url() -> str:
    return f"https://router.huggingface.co/hf-inference/models/{_model_id()}"


def _confidence_threshold() -> float:
    raw = str(os.getenv("CRYPTOBERT_CONFIDENCE_THRESHOLD", "0.60")).strip()
    try:
        value = float(raw)
    except ValueError:
        return 0.60

    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def _normalize_label(label: str) -> str:
    value = str(label).strip().upper()
    if value in {"LABEL_1", "POSITIVE", "BULLISH"}:
        return "LABEL_1"
    if value in {"LABEL_0", "NEGATIVE", "BEARISH"}:
        return "LABEL_0"
    return "UNCERTAIN"


def _extract_top_prediction(result: Any) -> tuple[str, float]:
    if isinstance(result, dict) and "label" in result:
        return str(result.get("label", "UNCERTAIN")), float(result.get("confidence", 0.0))

    predictions: list[dict[str, Any]] = []

    if isinstance(result, list) and result:
        first = result[0]
        if isinstance(first, list):
            predictions = [item for item in first if isinstance(item, dict)]
        elif isinstance(first, dict):
            predictions = [item for item in result if isinstance(item, dict)]

    if not predictions:
        raise ValueError("Unexpected CryptoBERT response payload")

    top_prediction = max(predictions, key=lambda item: float(item.get("score", 0.0)))
    return str(top_prediction.get("label", "UNCERTAIN")), float(top_prediction.get("score", 0.0))


def predict_cryptobert_sentiment(text: str, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    headers = {
        "x-wait-for-model": "true",
        "Content-Type": "application/json",
    }
    token = _hf_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    payload = {"inputs": text}

    try:
        response = _SESSION.post(
            _hf_url(),
            headers=headers,
            json=payload,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "unknown"
        detail = exc.response.text[:300] if exc.response is not None else str(exc)
        raise RuntimeError(f"CryptoBERT upstream returned status={status_code}: {detail}") from exc
    except requests.RequestException as exc:
        raise RuntimeError(f"CryptoBERT upstream request failed: {exc}") from exc

    raw_label, confidence = _extract_top_prediction(response.json())
    normalized_label = _normalize_label(raw_label)

    if confidence < _confidence_threshold():
        normalized_label = "UNCERTAIN"

    return {
        "label": normalized_label,
        "confidence": float(confidence),
    }
