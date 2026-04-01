import pandas as pd
from fastapi import FastAPI, HTTPException
import joblib
import numpy as np
from models.inference.realtime import RealtimePredictor
from api.schemas.request import PredictRequest, PredictEngineeredRequest

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


@app.post("/predict/base")
def predictBase(data: PredictRequest):

    predictor = RealtimePredictor()

    if len(data.data) == 0:
        raise HTTPException(
            status_code=400,
            detail="No data provided"
        )

    X = pd.DataFrame([list(feature.model_dump().values()) for feature in data.data], columns=data.data[0].model_dump().keys())
    print(X)
    prediction = predictor.predictBase(X)
   
    return {"prediction": prediction.tolist()}

@app.post("/predict/engineered")
def predictEngineered(data: PredictEngineeredRequest):

    predictor = RealtimePredictor()

    if len(data.data) == 0:
        raise HTTPException(
            status_code=400,
            detail="No data provided"
        )

    X = np.array([list(feature.model_dump().values()) for feature in data.data])
    
    prediction = predictor.predict(X)
   
    return {"prediction": int(prediction)}