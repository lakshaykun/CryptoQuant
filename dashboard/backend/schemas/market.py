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

class ReturnResponse(BaseModel):
    open_time       : datetime
    log_return      : float | None
    log_return_lag1 : float | None
    log_return_lag2 : float | None


class VolatilityResponse(BaseModel):
    open_time        : datetime
    volatility       : float | None
    volatility_5     : float | None
    volatility_ratio : float | None


class OrderFlowResponse(BaseModel):
    open_time       : datetime
    imbalance_ratio : float | None
    buy_ratio       : float | None
    buy_ratio_5     : float | None
    taker_buy_base  : float | None


class MomentumResponse(BaseModel):
    open_time      : datetime
    momentum       : float | None
    trend_strength : float | None
    volume_spike   : float | None


class MicrostructureResponse(BaseModel):
    open_time         : datetime
    body_size         : float | None
    price_range_ratio : float | None


class HeatmapResponse(BaseModel):
    hour        : int
    day_of_week : int
    avg_return  : float | None
    avg_volume  : float | None