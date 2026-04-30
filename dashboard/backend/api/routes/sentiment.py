from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import datetime
import asyncpg
from dashboard.backend.db.session import get_conn
from dashboard.backend.schemas.sentiment import SentimentTimelinePoint, SentimentBySource, SentimentSummary
from dashboard.backend.api.symbols import to_sentiment_symbol
from fastapi_cache.decorator import cache

router = APIRouter()


@router.get("/{symbol}/timeline", response_model=list[SentimentTimelinePoint])
async def get_sentiment_timeline(
    symbol    : str,
    from_time : datetime | None = Query(default=None, alias="from"),
    to_time   : datetime | None = Query(default=None, alias="to"),
    limit     : int = Query(default=500, le=2000),
    pool      : asyncpg.Pool = Depends(get_conn)
):
    sentiment_symbol = to_sentiment_symbol(symbol)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT window_start, sentiment_index, avg_confidence, message_count
                FROM sentiment_gold
                WHERE UPPER(symbol) = UPPER($1)
                  AND ($2::timestamptz IS NULL OR window_start >= $2)
                  AND ($3::timestamptz IS NULL OR window_start <= $3)
                ORDER BY window_start DESC
                LIMIT $4
            """, sentiment_symbol, from_time, to_time, limit)

        if not rows:
            raise HTTPException(status_code=404, detail=f"No sentiment data for {symbol}")

        return [SentimentTimelinePoint(**dict(r)) for r in rows]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{symbol}/by-source", response_model=list[SentimentBySource])
@cache(expire=120)
async def get_sentiment_by_source(
    symbol    : str,
    from_time : datetime | None = Query(default=None, alias="from"),
    to_time   : datetime | None = Query(default=None, alias="to"),
    pool      : asyncpg.Pool = Depends(get_conn)
):
    sentiment_symbol = to_sentiment_symbol(symbol)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    source,
                    COUNT(*)        AS message_count,
                    SUM(engagement) AS total_engagement
                FROM sentiment_silver
                WHERE UPPER(symbol) = UPPER($1)
                  AND ($2::timestamptz IS NULL OR event_time >= $2)
                  AND ($3::timestamptz IS NULL OR event_time <= $3)
                GROUP BY source
                ORDER BY message_count DESC
            """, sentiment_symbol, from_time, to_time)

        if not rows:
            raise HTTPException(status_code=404, detail=f"No source data for {symbol}")

        return [SentimentBySource(**dict(r)) for r in rows]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{symbol}/summary", response_model=SentimentSummary)
@cache(expire=30)
async def get_sentiment_summary(
    symbol: str,
    pool  : asyncpg.Pool = Depends(get_conn)
):
    sentiment_symbol = to_sentiment_symbol(symbol)
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    symbol,
                    sentiment_index AS latest_sentiment,
                    avg_confidence,
                    message_count   AS total_messages,
                    window_start    AS latest_time
                FROM sentiment_gold
                WHERE UPPER(symbol) = UPPER($1)
                ORDER BY window_start DESC
                LIMIT 1
            """, sentiment_symbol)

        if not row:
            raise HTTPException(status_code=404, detail=f"No sentiment summary for {symbol}")

        return SentimentSummary(**dict(row))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
