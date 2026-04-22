import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from models.inference.realtime import RealtimePredictor
from api.schemas.request import PredictRequest

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

    return {"prediction": np.asarray(prediction).tolist()}