import time

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from models.inference.realtime import RealtimePredictor
from api.schemas.request import PredictRequest

app = FastAPI(
    title="CryptoQuant",
    description="API for predicting Crypto Returns",
    version="1.0.0"
)


REQUEST_COUNT = Counter(
    "cryptoquant_api_requests_total",
    "Total number of API requests.",
    ["method", "path", "status"],
)
REQUEST_LATENCY = Histogram(
    "cryptoquant_api_request_latency_seconds",
    "API request latency in seconds.",
    ["method", "path"],
)
PREDICTION_COUNT = Counter(
    "cryptoquant_api_predictions_total",
    "Total number of predictions returned by the API.",
)
PREDICTION_ERROR = Histogram(
    "cryptoquant_api_prediction_error",
    "Absolute prediction error when ground truth is provided.",
    buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 1.0),
)
PREDICTION_ERROR_COUNT = Counter(
    "cryptoquant_api_prediction_error_observations_total",
    "Number of prediction error observations.",
)
PREDICTION_MAE_LAST = Gauge(
    "cryptoquant_api_prediction_mae_last",
    "Mean absolute error from the most recent request that included actuals.",
)


@app.middleware("http")
async def record_http_metrics(request: Request, call_next):
    start_time = time.perf_counter()
    status_code = 500

    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        latency = time.perf_counter() - start_time
        path = request.url.path
        method = request.method
        REQUEST_LATENCY.labels(method=method, path=path).observe(latency)
        REQUEST_COUNT.labels(method=method, path=path, status=str(status_code)).inc()


@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/")
def health_check():
    return {
        "status": "running"
    }

@app.post("/predict")
def predict(data: PredictRequest):
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

    predictions = np.asarray(prediction, dtype=float).reshape(-1)
    PREDICTION_COUNT.inc(int(len(predictions)))

    if "actual_log_return_lead1" in X.columns:
        actual = pd.to_numeric(X["actual_log_return_lead1"], errors="coerce")
        mask = actual.notna().to_numpy()

        if mask.any():
            observed_actual = actual.to_numpy()[mask]
            observed_predictions = predictions[mask]
            absolute_errors = np.abs(observed_predictions - observed_actual)

            for value in absolute_errors:
                PREDICTION_ERROR.observe(float(value))

            PREDICTION_ERROR_COUNT.inc(int(absolute_errors.size))
            PREDICTION_MAE_LAST.set(float(absolute_errors.mean()))

    return {"prediction": predictions.tolist()}