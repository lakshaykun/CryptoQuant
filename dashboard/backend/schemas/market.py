from pydantic import BaseModel
from datetime import datetime


class CandleResponse(BaseModel):
    open_time   : datetime
    open        : float | None
    high        : float | None
    low         : float | None
    close       : float | None
    volume      : float | None
    trades      : int | None
    ma_5        : float | None
    ma_20       : float | None

    class Config:
        from_attributes = True