import os
from pathlib import Path

import requests
from utils.env import load_env_file

# Ensure configs/app.env is loaded for local runs.
load_env_file()


def _token_from_app_env() -> str:
    env_path = Path("configs/app.env")
    if not env_path.exists():
        return ""

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        if key.strip() == "HUGGINGFACE_API_KEY":
            return value.strip().strip('"').strip("'")

    return ""


HF_TOKEN = _token_from_app_env() or os.getenv("HUGGINGFACE_API_KEY", "")
HF_TOKEN = HF_TOKEN.strip().strip('"').strip("'")
MODEL_ID = "kk08/CryptoBERT"

API_URL = f"https://router.huggingface.co/hf-inference/models/{MODEL_ID}"

HEADERS = {
    "Authorization": f"Bearer {HF_TOKEN}",
    "x-wait-for-model": "true",
    "Content-Type": "application/json"
}

def predict_sentiment_api(text: str) -> dict:
    payload = {"inputs": text}
    
    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
        
        if response.status_code != 200:
            return {
                "label": "ERROR", 
                "confidence": 0.0, 
                "detail": f"Status {response.status_code}: {response.text}"
            }

        result = response.json()
        
        if isinstance(result, list) and len(result) > 0:
            predictions = result[0]
            top_prediction = max(predictions, key=lambda x: x['score'])
            
            label = top_prediction["label"]
            confidence = float(top_prediction["score"])

            if confidence < 0.60:
                label = "UNCERTAIN"

            return {
                "label": label,
                "confidence": confidence
            }
        
        return {
            "label": "ERROR",
            "confidence": 0.0,
            "detail": "Unexpected API response format",
        }

    except Exception as e:
        return {"label": "ERROR", "confidence": 0.0, "detail": str(e)}


def predict_sentiment(text: str) -> dict:
    return predict_sentiment_api(text)
# Example usage: