import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException

try:
    from deltalake import DeltaTable
except Exception:  # pragma: no cover - handled at runtime
    DeltaTable = None

from utils_global.config_loader import load_config
from utils_global.logger import get_logger

from models.inference.realtime import RealtimePredictor
from api.schemas.request import PredictRequest, PredictResponse

app = FastAPI(
    title="CryptoQuant",
    description="API for predicting Crypto Returns",
    version="1.0.0"
)
logger = get_logger("api")


def _load_drift_frame() -> pd.DataFrame:
    if DeltaTable is None:
        raise HTTPException(
            status_code=503,
            detail="deltalake is not available in this API runtime",
        )

    model_config = load_config("configs/model.yaml") or {}
    history_path = (model_config.get("monitoring") or {}).get("history", {}).get("path")

    if not history_path:
        raise HTTPException(status_code=500, detail="Monitoring history path is not configured")

    try:
        table = DeltaTable(history_path)
        available_columns = list(table.schema().to_pyarrow().names)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Unable to load drift history from '{history_path}': {exc}",
        ) from exc

    if not available_columns:
        return pd.DataFrame()

    selected_columns = [
        column
        for column in [
            "timestamp",
            "event_time",
            "feature_name",
            "drift_type",
            "drift_score",
            "drift_detected",
            "model_metric",
            "overall_drift_score",
            "data_drift_score",
            "model_drift_score",
            "triggered",
            "trigger_reason",
        ]
        if column in available_columns
    ]

    if not selected_columns:
        return pd.DataFrame()

    return table.to_pandas(columns=selected_columns)

@app.get("/")
def health_check():
    return {
        "status": "running"
    }


@app.get("/drift")
def get_drift_summary():
    frame = _load_drift_frame()
    if frame.empty:
        return {
            "status": "ok",
            "summary": None,
            "top_features": [],
        }

    time_column = "timestamp" if "timestamp" in frame.columns else "event_time"
    if time_column not in frame.columns:
        raise HTTPException(status_code=500, detail="Drift history is missing a timestamp column")

    frame = frame.copy()
    frame[time_column] = pd.to_datetime(frame[time_column], errors="coerce", utc=True)
    frame = frame.dropna(subset=[time_column])

    if frame.empty:
        return {
            "status": "ok",
            "summary": None,
            "top_features": [],
        }

    latest_timestamp = frame[time_column].max()
    latest = frame[frame[time_column] == latest_timestamp]

    overall = pd.DataFrame()
    if "feature_name" in latest.columns:
        overall = latest[latest["feature_name"] == "__overall__"]

    if overall.empty:
        drift_detected = bool(latest.get("drift_detected", pd.Series(dtype=bool)).fillna(False).any())
        overall_score = float(pd.to_numeric(latest.get("drift_score"), errors="coerce").max())
        data_score = overall_score
        model_score = float("nan")
        triggered = bool(latest.get("triggered", pd.Series(dtype=bool)).fillna(False).any())
        trigger_reason = "unknown"
    else:
        latest_overall = overall.sort_values(time_column).iloc[-1]
        drift_detected = bool(latest_overall.get("drift_detected", False))
        overall_score = float(latest_overall.get("overall_drift_score", latest_overall.get("drift_score", 0.0)))
        data_score = float(latest_overall.get("data_drift_score", overall_score))
        model_score = float(latest_overall.get("model_drift_score", latest_overall.get("model_metric", 0.0)))
        triggered = bool(latest_overall.get("triggered", False))
        trigger_reason = str(latest_overall.get("trigger_reason", "unknown"))

    feature_rows = latest
    if "feature_name" in latest.columns:
        feature_rows = latest[~latest["feature_name"].isin(["__overall__", "__model__"])]

    if not feature_rows.empty and "drift_score" in feature_rows.columns:
        feature_rows = feature_rows.sort_values("drift_score", ascending=False)

    top_features = []
    for _, row in feature_rows.head(8).iterrows():
        top_features.append(
            {
                "feature_name": str(row.get("feature_name", "unknown_feature")),
                "drift_score": float(row.get("drift_score", 0.0)),
                "drift_detected": bool(row.get("drift_detected", False)),
            }
        )

    logger.info(
        "[api_drift] timestamp=%s overall=%.4f data=%.4f model=%.4f detected=%s triggered=%s",
        latest_timestamp.isoformat(),
        overall_score,
        data_score,
        model_score,
        drift_detected,
        triggered,
    )

    return {
        "status": "ok",
        "summary": {
            "timestamp": latest_timestamp.isoformat(),
            "drift_detected": drift_detected,
            "overall_drift_score": overall_score,
            "data_drift_score": data_score,
            "model_drift_score": model_score,
            "triggered": triggered,
            "trigger_reason": trigger_reason,
        },
        "top_features": top_features,
    }

@app.post("/predict")
def predict(data: PredictRequest) -> PredictResponse:
    if len(data.data) == 0:
        raise HTTPException(
            status_code=400,
            detail="No data provided"
        )

    features = [feature.model_dump() for feature in data.data]
    X = pd.DataFrame(features)

    if X.empty:
        raise HTTPException(
            status_code=400,
            detail="No data provided"
        )

    try:
        predictor = RealtimePredictor()
        prediction = predictor.predict(X)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if isinstance(prediction, dict) and "predictions" in prediction:
        return prediction

    predictions = np.asarray(prediction, dtype=float).reshape(-1)
    return {"predictions": {"prediction": predictions.tolist()}}