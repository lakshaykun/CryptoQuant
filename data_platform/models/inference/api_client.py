import json
import os
from urllib import error, request


DEFAULT_PREDICTION_API_URL = os.getenv("PREDICTION_API_URL", "http://api:8000/predict")
DEFAULT_PREDICTION_API_TIMEOUT_SECONDS = float(os.getenv("PREDICTION_API_TIMEOUT_SECONDS", "30"))


def predict_with_api(df, api_url=None, timeout_seconds=None):
    prediction_api_url = api_url or DEFAULT_PREDICTION_API_URL
    request_timeout = DEFAULT_PREDICTION_API_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds

    payload = {"data": json.loads(df.to_json(orient="records", date_format="iso"))}
    body = json.dumps(payload).encode("utf-8")

    api_request = request.Request(
        prediction_api_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(api_request, timeout=request_timeout) as response:
            response_body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Prediction API returned HTTP {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Unable to reach prediction API at '{prediction_api_url}': {exc.reason}") from exc

    try:
        response_payload = json.loads(response_body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Prediction API returned invalid JSON") from exc

    if isinstance(response_payload.get("predictions"), dict):
        return response_payload["predictions"]

    predictions = response_payload.get("prediction")
    if not isinstance(predictions, list):
        raise RuntimeError("Prediction API response did not include prediction fields")

    return {"prediction": predictions}