from pydantic import BaseModel
from datetime import datetime


class SentimentTimelinePoint(BaseModel):
    window_start    : datetime
    sentiment_index : float | None
    avg_confidence  : float | None
    message_count   : int | None


class SentimentBySource(BaseModel):
    source          : str
    message_count   : int
    total_engagement: int | None


class SentimentSummary(BaseModel):
    symbol          : str
    latest_sentiment: float | None
    avg_confidence  : float | None
    total_messages  : int | None
    latest_time     : datetime | None


class SentimentCorrelation(BaseModel):
    lag         : int
    correlation : float | None