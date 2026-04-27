from fastapi import APIRouter, Query, HTTPException, Depends
import asyncpg
from dashboard.backend.db.session import get_conn
from dashboard.backend.schemas.predictions import ForecastPoint, AccuracyResponse
from fastapi_cache.decorator import cache

router = APIRouter()


@router.get("/{symbol}/forecast", response_model=list[ForecastPoint])
@cache(expire=60)
async def get_forecast(
    symbol: str,
    limit : int = Query(default=15, le=60),
    pool  : asyncpg.Pool = Depends(get_conn)
):
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT open_time, prediction, predicted_close
                FROM predictions
                WHERE symbol          = $1
                  AND predicted_close IS NOT NULL
                ORDER BY open_time DESC
                LIMIT $2
            """, symbol.upper(), limit)

        if not rows:
            raise HTTPException(status_code=404, detail=f"No predictions found for {symbol}")

        return [ForecastPoint(**dict(r)) for r in rows]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{symbol}/accuracy", response_model=AccuracyResponse)
@cache(expire=300)
async def get_accuracy(
    symbol: str,
    limit : int = Query(default=100, le=500),
    pool  : asyncpg.Pool = Depends(get_conn)
):
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    COUNT(*)                                             AS total_predictions,
                    COUNT(*) FILTER (WHERE actual_close IS NOT NULL)    AS matched_predictions,
                    AVG(ABS(error))                                      AS mae,
                    AVG(CASE WHEN direction_correct THEN 1.0
                             ELSE 0.0 END) * 100                         AS directional_accuracy
                FROM (
                    SELECT error, actual_close, direction_correct
                    FROM predictions
                    WHERE symbol       = $1
                      AND actual_close IS NOT NULL
                    ORDER BY open_time DESC
                    LIMIT $2
                ) recent
            """, symbol.upper(), limit)

        if not row or row["total_predictions"] == 0:
            raise HTTPException(status_code=404, detail=f"No accuracy data for {symbol}")

        return AccuracyResponse(**dict(row))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))