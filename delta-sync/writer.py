import asyncpg
import pandas as pd
from pipelines.utils.logger import get_logger

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