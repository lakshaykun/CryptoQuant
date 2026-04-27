from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from sqlalchemy import Double, Integer, Boolean, Date, Text
from datetime import datetime, date as date_type
import sqlalchemy as sa

class Base(DeclarativeBase):
    pass

class Market(Base):
    __tablename__ = "market"

    open_time            : Mapped[datetime] = mapped_column(sa.TIMESTAMP(timezone=True), primary_key=True)
    symbol               : Mapped[str]      = mapped_column(Text, primary_key=True)
    open                 : Mapped[float]    = mapped_column(Double, nullable=True)
    high                 : Mapped[float]    = mapped_column(Double, nullable=True)
    low                  : Mapped[float]    = mapped_column(Double, nullable=True)
    close                : Mapped[float]    = mapped_column(Double, nullable=True)
    volume               : Mapped[float]    = mapped_column(Double, nullable=True)
    trades               : Mapped[int]      = mapped_column(Integer, nullable=True)
    taker_buy_base       : Mapped[float]    = mapped_column(Double, nullable=True)
    log_return           : Mapped[float]    = mapped_column(Double, nullable=True)
    volatility           : Mapped[float]    = mapped_column(Double, nullable=True)
    imbalance_ratio      : Mapped[float]    = mapped_column(Double, nullable=True)
    buy_ratio            : Mapped[float]    = mapped_column(Double, nullable=True)
    log_return_lag1      : Mapped[float]    = mapped_column(Double, nullable=True)
    log_return_lag2      : Mapped[float]    = mapped_column(Double, nullable=True)
    buy_ratio_lag1       : Mapped[float]    = mapped_column(Double, nullable=True)
    ma_5                 : Mapped[float]    = mapped_column(Double, nullable=True)
    ma_20                : Mapped[float]    = mapped_column(Double, nullable=True)
    volatility_5         : Mapped[float]    = mapped_column(Double, nullable=True)
    volume_5             : Mapped[float]    = mapped_column(Double, nullable=True)
    buy_ratio_5          : Mapped[float]    = mapped_column(Double, nullable=True)
    momentum             : Mapped[float]    = mapped_column(Double, nullable=True)
    volume_spike         : Mapped[float]    = mapped_column(Double, nullable=True)
    price_range_ratio    : Mapped[float]    = mapped_column(Double, nullable=True)
    body_size            : Mapped[float]    = mapped_column(Double, nullable=True)
    hour                 : Mapped[int]      = mapped_column(Integer, nullable=True)
    day_of_week          : Mapped[int]      = mapped_column(Integer, nullable=True)
    trend_strength       : Mapped[float]    = mapped_column(Double, nullable=True)
    volatility_ratio     : Mapped[float]    = mapped_column(Double, nullable=True)
    is_valid_feature_row : Mapped[bool]     = mapped_column(Boolean, nullable=True)
    date                 : Mapped[date_type]     = mapped_column(Date, nullable=True)
    ingestion_time       : Mapped[datetime] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)

class Prediction(Base):
    __tablename__ = "predictions"

    open_time        : Mapped[datetime]  = mapped_column(sa.TIMESTAMP(timezone=True), primary_key=True)
    symbol           : Mapped[str]       = mapped_column(Text, primary_key=True)
    date             : Mapped[date_type] = mapped_column(Date, nullable=True)
    prediction       : Mapped[float]     = mapped_column(Double, nullable=True)
    ingestion_time   : Mapped[datetime]  = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)
    predicted_close  : Mapped[float]     = mapped_column(Double, nullable=True)  # close * exp(prediction)
    actual_close     : Mapped[float]     = mapped_column(Double, nullable=True)  # filled in later
    error            : Mapped[float]     = mapped_column(Double, nullable=True)  # actual - predicted, filled in later
    direction_correct: Mapped[bool]      = mapped_column(Boolean, nullable=True) # filled in later