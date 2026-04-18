import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from api.cryptobert_service import predict_cryptobert_sentiment
from api.schemas.request import (
    CryptoFeatures,
    PredictEngineeredRequest,
    SentimentPredictRequest,
    SentimentPredictResponse,
)

app = FastAPI(
    title="CryptoQuant",
    description="API for predicting Crypto Returns",
    version="1.0.0"
)

@app.get("/")
def health_check():
    return {
        "status": "running"
    }


@app.post("/predict", response_model=SentimentPredictResponse)
def predictSentiment(data: SentimentPredictRequest):
    text = data.text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text cannot be empty")

    try:
        prediction = predict_cryptobert_sentiment(text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return SentimentPredictResponse(**prediction)


@app.post("/predict/base")
def predictBase(data: list[CryptoFeatures]):
    try:
        from models.inference.realtime import RealtimePredictor
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Predictor import failed: {exc}") from exc

    predictor = RealtimePredictor()

    if len(data) == 0:
        raise HTTPException(
            status_code=400,
            detail="No data provided"
        )

    # Convert to list of dicts
    records = [feature.model_dump() for feature in data]

    # Create DataFrame
    X = pd.DataFrame(records)

    if X.empty:
        raise HTTPException(
            status_code=400,
            detail="No data provided"
        )

    try:
        prediction = predictor.predictBase(X)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"prediction": np.asarray(prediction).tolist()}

@app.post("/predict/engineered")
def predictEngineered(data: PredictEngineeredRequest):
    try:
        from models.inference.realtime import RealtimePredictor
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Predictor import failed: {exc}") from exc

    predictor = RealtimePredictor()

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
        prediction = predictor.predict(X)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"prediction": np.asarray(prediction).tolist()}