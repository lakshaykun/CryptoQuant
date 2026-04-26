from datetime import datetime

from pydantic import BaseModel
from typing import List


class BaseCryptoFeatures(BaseModel):
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: datetime
    quote_volume: float
    symbol: str = "BTCUSDT"
    trades: int
    taker_buy_base: float
    taker_buy_quote: float

class BaseModelData(BaseModel):
    data: List[BaseCryptoFeatures]
