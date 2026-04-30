from pydantic import BaseModel, ConfigDict
from typing import List
from datetime import datetime
from uuid import UUID

# --- Portfolios ---
class PortfolioCreate(BaseModel):
    name: str

class PortfolioResponse(BaseModel):
    id: UUID
    name: str
    created_at: datetime
    position_count: int

    model_config = ConfigDict(from_attributes=True)

# --- Positions ---
class PositionCreate(BaseModel):
    symbol: str
    quantity: float
    buy_price: float

class PositionUpdate(BaseModel):
    quantity: float
    buy_price: float

class PositionResponse(BaseModel):
    symbol: str
    quantity: float
    buy_price: float
    opened_at: datetime

    model_config = ConfigDict(from_attributes=True)

class PositionWithPnL(PositionResponse):
    current_price: float | None
    pnl_pct: float | None
    pnl_value: float | None
    current_value: float | None

class PortfolioDetail(BaseModel):
    id: UUID
    name: str
    created_at: datetime
    positions: List[PositionWithPnL]
    total_value: float | None
    total_pnl_value: float | None

    model_config = ConfigDict(from_attributes=True)

# --- Analytics ---
class RiskItem(BaseModel):
    symbol: str
    volatility_5: float | None
    position_value: float | None
    weighted_risk: float | None

class RiskResponse(BaseModel):
    positions: List[RiskItem]
    total_portfolio_risk: float | None

class ScoreItem(BaseModel):
    symbol: str
    predicted_log_return: float | None
    volatility_5: float | None
    sentiment_index: float | None
    score: float | None
    in_portfolio: bool

class SuggestionItem(BaseModel):
    symbol: str
    score: float | None
    suggested_weight_pct: float | None
    current_weight_pct: float | None
    action: str
