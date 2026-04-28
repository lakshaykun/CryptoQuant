from fastapi import APIRouter, Query, HTTPException, Depends
import asyncpg
from dashboard.backend.db.session import get_conn
from dashboard.backend.schemas.sentiment import SentimentCorrelation
from fastapi_cache.decorator import cache
import pandas as pd

router = APIRouter()

@router.get("/{symbol}/sentiment-correlation", response_model=list[SentimentCorrelation])
@cache(expire=300)
async def get_sentiment_correlation(
    symbol   : str,
    max_lag  : int = Query(default=10, le=20),
    pool     : asyncpg.Pool = Depends(get_conn)
):
    """
    Computes cross-correlation between sentiment_index and log_return
    at lags from -max_lag to +max_lag.
    Negative lag = sentiment leads price.
    Positive lag = sentiment lags price.
    """
    try:
        async with pool.acquire() as conn:
            # fetch aligned sentiment + returns on the same time axis
            rows = await conn.fetch("""
                SELECT
                    s.window_start,
                    s.sentiment_index,
                    m.log_return
                FROM sentiment_gold s
                JOIN market m
                  ON s.symbol      = m.symbol
                 AND s.window_start = m.open_time
                WHERE s.symbol = $1
                  AND s.sentiment_index IS NOT NULL
                  AND m.log_return      IS NOT NULL
                  AND m.is_valid_feature_row = TRUE
                ORDER BY s.window_start ASC
            """, symbol.upper())

        if not rows or len(rows) < max_lag * 2:
            raise HTTPException(status_code=404, detail=f"Not enough data for correlation on {symbol}")

        df = pd.DataFrame(rows, columns=["window_start", "sentiment_index", "log_return"])

        results = []
        for lag in range(-max_lag, max_lag + 1):
            if lag < 0:
                # sentiment leads price — shift sentiment forward
                corr = df["sentiment_index"].shift(-lag).corr(df["log_return"])
            else:
                # sentiment lags price — shift returns forward
                corr = df["sentiment_index"].corr(df["log_return"].shift(-lag))

            results.append(SentimentCorrelation(
                lag=lag,
                correlation=round(corr, 4) if pd.notna(corr) else None
            ))

        return results

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))