from fastapi import FastAPI
from pydantic import BaseModel
from model_server.cryptobert_service import predict_sentiment
app = FastAPI(
    title="CryptoBERT Inference API",
    version="1.0.0"
)


class PredictRequest(BaseModel):
    text: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict")
def predict(req: PredictRequest):
    return predict_sentiment(req.text)