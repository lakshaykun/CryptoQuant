from pydantic import BaseModel
from datetime import datetime


class ForecastPoint(BaseModel):
    open_time       : datetime
    prediction      : float | None
    predicted_close : float | None


class AccuracyResponse(BaseModel):
    total_predictions    : int
    matched_predictions  : int
    mae                  : float | None
    directional_accuracy : float | None