from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import datetime, timezone
import asyncpg
from dashboard.backend.db.session import get_conn
from dashboard.backend.schemas.market import CandleResponse

router = APIRouter()

VALID_INTERVALS = {"1m", "5m", "1h", "1d"}

# raw market table for now, replace with continuous agg tables later
INTERVAL_TABLE = {
    "1m": "market",
    "5m": "market",
    "1h": "market",
    "1d": "market",
}


@router.get("/{symbol}", response_model=list[CandleResponse])
async def get_candles(
    symbol   : str,
    interval : str      = Query(default="1m"),
    from_time: datetime | None = Query(default=None, alias="from"),
    to_time  : datetime | None = Query(default=None, alias="to"),
    limit    : int      = Query(default=500, le=1000),
    pool     : asyncpg.Pool = Depends(get_conn)
):
    if interval not in VALID_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid interval. Must be one of {VALID_INTERVALS}"
        )

    table = INTERVAL_TABLE[interval]

    query = f"""
        SELECT
            open_time, open, high, low, close, volume, trades, ma_5, ma_20
        FROM {table}
        WHERE symbol = $1
          AND is_valid_feature_row = TRUE
          AND ($2::timestamptz IS NULL OR open_time >= $2)
          AND ($3::timestamptz IS NULL OR open_time <= $3)
        ORDER BY open_time DESC
        LIMIT $4
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, symbol.upper(), from_time, to_time, limit)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No candles found for {symbol}"
            )

        return [CandleResponse(**dict(row)) for row in rows]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))