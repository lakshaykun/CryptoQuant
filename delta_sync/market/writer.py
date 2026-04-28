import asyncpg
import pandas as pd
import math
from data_platform.utils_global.logger import get_logger

logger = get_logger(__name__)

INSERT_SQL = """
    INSERT INTO market (
        open_time, symbol, open, high, low, close, volume, trades,
        taker_buy_base, log_return, volatility, imbalance_ratio,
        buy_ratio, log_return_lag1, log_return_lag2, buy_ratio_lag1,
        ma_5, ma_20, volatility_5, volume_5, buy_ratio_5, momentum,
        volume_spike, price_range_ratio, body_size, hour, day_of_week,
        trend_strength, volatility_ratio, is_valid_feature_row,
        date, ingestion_time
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
        $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
        $23, $24, $25, $26, $27, $28, $29, $30, $31, $32
    )
    ON CONFLICT (open_time, symbol) DO NOTHING
"""

COLUMNS = [
    "open_time", "symbol", "open", "high", "low", "close", "volume", "trades",
    "taker_buy_base", "log_return", "volatility", "imbalance_ratio",
    "buy_ratio", "log_return_lag1", "log_return_lag2", "buy_ratio_lag1",
    "ma_5", "ma_20", "volatility_5", "volume_5", "buy_ratio_5", "momentum",
    "volume_spike", "price_range_ratio", "body_size", "hour", "day_of_week",
    "trend_strength", "volatility_ratio", "is_valid_feature_row",
    "date", "ingestion_time"
]


async def upsert_market_rows(df: pd.DataFrame, conn: asyncpg.Connection) -> int:
    """
    Upserts a DataFrame into the market hypertable.
    Returns number of rows attempted.
    """
    if df.empty:
        logger.info("[writer] No rows to write")
        return 0

    try:
        df = df[COLUMNS].copy()

        # convert rows to native python types for asyncpg
        records = [
            tuple(row)
            for row in df.itertuples(index=False, name=None)
        ]

        await conn.executemany(INSERT_SQL, records)

        logger.info(f"[writer] Upserted {len(records)} rows")
        return len(records)

    except Exception as e:
        logger.error(f"[writer] Upsert failed → {e}")
        raise

PREDICTIONS_INSERT_SQL = """
    INSERT INTO predictions (
        open_time, symbol, date, prediction, ingestion_time, predicted_close
    ) VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (open_time, symbol) DO NOTHING
"""

PREDICTIONS_COLUMNS = ["open_time", "symbol", "date", "prediction", "ingestion_time"]

FILL_ACTUALS_SQL = """
    UPDATE predictions p
    SET
        actual_close      = m.close,
        error             = m.close - p.predicted_close,
        direction_correct = (
            (p.predicted_close > m_prev.close) = (m.close > m_prev.close)
        )
    FROM market m
    JOIN market m_prev
      ON m_prev.symbol    = m.symbol
     AND m_prev.open_time = (
         SELECT MAX(open_time) FROM market
         WHERE symbol    = m.symbol
           AND open_time < m.open_time
     )
    WHERE p.symbol       = m.symbol
      AND p.open_time    = m.open_time
      AND p.actual_close IS NULL
"""

async def upsert_prediction_rows(df: pd.DataFrame, conn: asyncpg.Connection) -> int:
    if df.empty:
        logger.info("[writer] No prediction rows to write")
        return 0

    try:
        df = df[PREDICTIONS_COLUMNS].copy()

        # fetch current closes to compute predicted_close at write time
        symbols = df["symbol"].unique().tolist()
        market_closes = await conn.fetch("""
            SELECT symbol, open_time, close FROM market
            WHERE symbol = ANY($1::text[])
        """, symbols)

        close_map = {
            (r["symbol"], r["open_time"]): r["close"]
            for r in market_closes
        }

        records = []
        for row in df.itertuples(index=False, name=None):
            open_time, symbol, date, prediction, ingestion_time = row
            current_close = close_map.get((symbol, open_time))
            predicted_close = current_close * math.exp(prediction) if current_close else None
            records.append((open_time, symbol, date, prediction, ingestion_time, predicted_close))

        await conn.executemany(PREDICTIONS_INSERT_SQL, records)
        logger.info(f"[writer] Upserted {len(records)} prediction rows")

        # fill actuals for any past predictions that now have market data
        await conn.execute(FILL_ACTUALS_SQL)
        logger.info("[writer] Filled in actuals for past predictions")

        return len(records)

    except Exception as e:
        logger.error(f"[writer] Prediction upsert failed → {e}")
        raise
