from fastapi import APIRouter, Query, HTTPException, Depends
from datetime import datetime
import asyncpg
from dashboard.backend.db.session import get_conn
from dashboard.backend.schemas.market import (
    ReturnResponse,
    VolatilityResponse,
    OrderFlowResponse,
    MomentumResponse,
    MicrostructureResponse,
    HeatmapResponse
)
from fastapi_cache.decorator import cache

router = APIRouter()

# shared filter helper to add open_time and close_time conditions to all queries, plus symbol and valid row check
def build_time_filter(from_time, to_time) -> tuple[str, list]:
    """Builds WHERE clause additions and param list for time filtering."""
    conditions = ["symbol = $1", "is_valid_feature_row = TRUE"]
    params = []
    idx = 2

    if from_time:
        conditions.append(f"open_time >= ${idx}")
        params.append(from_time)
        idx += 1

    if to_time:
        conditions.append(f"open_time <= ${idx}")
        params.append(to_time)
        idx += 1

    return " AND ".join(conditions), params, idx


# returns log_return, log_return_lag1, log_return_lag2 for return histogram and autocorrelation analysis
@router.get("/{symbol}/returns", response_model=list[ReturnResponse])
async def get_returns(
    symbol    : str,
    from_time : datetime | None = Query(default=None, alias="from"),
    to_time   : datetime | None = Query(default=None, alias="to"),
    limit     : int = Query(default=1000, le=5000),
    pool      : asyncpg.Pool = Depends(get_conn)
):
    where, params, idx = build_time_filter(from_time, to_time)

    query = f"""
        SELECT open_time, log_return, log_return_lag1, log_return_lag2
        FROM market
        WHERE {where}
        ORDER BY open_time DESC
        LIMIT ${idx}
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, symbol.upper(), *params, limit)
        if not rows:
            raise HTTPException(status_code=404, detail=f"No return data for {symbol}")
        return [ReturnResponse(**dict(r)) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# returns volatility for volatility ribbon
@router.get("/{symbol}/volatility", response_model=list[VolatilityResponse])
async def get_volatility(
    symbol    : str,
    from_time : datetime | None = Query(default=None, alias="from"),
    to_time   : datetime | None = Query(default=None, alias="to"),
    limit     : int = Query(default=500, le=2000),
    pool      : asyncpg.Pool = Depends(get_conn)
):
    where, params, idx = build_time_filter(from_time, to_time)

    query = f"""
        SELECT open_time, volatility, volatility_5, volatility_ratio
        FROM market
        WHERE {where}
        ORDER BY open_time DESC
        LIMIT ${idx}
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, symbol.upper(), *params, limit)
        if not rows:
            raise HTTPException(status_code=404, detail=f"No volatility data for {symbol}")
        return [VolatilityResponse(**dict(r)) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# returns order flow (ratio of buyers and takers) for order flow charts
@router.get("/{symbol}/orderflow", response_model=list[OrderFlowResponse])
async def get_orderflow(
    symbol    : str,
    from_time : datetime | None = Query(default=None, alias="from"),
    to_time   : datetime | None = Query(default=None, alias="to"),
    limit     : int = Query(default=200, le=1000),
    pool      : asyncpg.Pool = Depends(get_conn)
):
    where, params, idx = build_time_filter(from_time, to_time)

    query = f"""
        SELECT open_time, imbalance_ratio, buy_ratio, buy_ratio_5, taker_buy_base
        FROM market
        WHERE {where}
        ORDER BY open_time DESC
        LIMIT ${idx}
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, symbol.upper(), *params, limit)
        if not rows:
            raise HTTPException(status_code=404, detail=f"No order flow data for {symbol}")
        return [OrderFlowResponse(**dict(r)) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# returns momentum and trend strength for momentum profile
@router.get("/{symbol}/momentum", response_model=list[MomentumResponse])
async def get_momentum(
    symbol    : str,
    from_time : datetime | None = Query(default=None, alias="from"),
    to_time   : datetime | None = Query(default=None, alias="to"),
    limit     : int = Query(default=200, le=1000),
    pool      : asyncpg.Pool = Depends(get_conn)
):
    where, params, idx = build_time_filter(from_time, to_time)

    query = f"""
        SELECT open_time, momentum, trend_strength, volume_spike
        FROM market
        WHERE {where}
        ORDER BY open_time DESC
        LIMIT ${idx}
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, symbol.upper(), *params, limit)
        if not rows:
            raise HTTPException(status_code=404, detail=f"No momentum data for {symbol}")
        return [MomentumResponse(**dict(r)) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# returns microstructure for heatmap and timeseries analysis. If mode=heatmap, returns pre-aggregated hourly microstructure stats for heatmap visualization. If mode=timeseries, returns raw microstructure features for time series analysis and scatter plots.
@router.get("/{symbol}/microstructure", response_model=list[MicrostructureResponse] | list[HeatmapResponse])
@cache(expire=60)
async def get_microstructure(
    symbol    : str,
    mode      : str = Query(default="timeseries", pattern="^(timeseries|heatmap)$"),
    from_time : datetime | None = Query(default=None, alias="from"),
    to_time   : datetime | None = Query(default=None, alias="to"),
    limit     : int = Query(default=500, le=2000),
    pool      : asyncpg.Pool = Depends(get_conn)
):
    try:
        async with pool.acquire() as conn:

            if mode == "heatmap":
                # pre-aggregated by hour + day_of_week, max 168 rows
                # no limit needed — 24*7 = 168 rows max
                query = """
                    SELECT
                        hour,
                        day_of_week,
                        AVG(log_return) AS avg_return,
                        AVG(volume)     AS avg_volume
                    FROM market
                    WHERE UPPER(symbol) = UPPER($1)
                      AND is_valid_feature_row = TRUE
                      AND ($2::timestamptz IS NULL OR open_time >= $2)
                      AND ($3::timestamptz IS NULL OR open_time <= $3)
                    GROUP BY hour, day_of_week
                    ORDER BY day_of_week, hour
                """
                rows = await conn.fetch(query, symbol.upper(), from_time, to_time)
                return [HeatmapResponse(**dict(r)) for r in rows]

            else:
                where, params, idx = build_time_filter(from_time, to_time)
                query = f"""
                    SELECT open_time, body_size, price_range_ratio
                    FROM market
                    WHERE {where}
                    ORDER BY open_time DESC
                    LIMIT ${idx}
                """
                rows = await conn.fetch(query, symbol.upper(), *params, limit)
                if not rows:
                    raise HTTPException(status_code=404, detail=f"No microstructure data for {symbol}")
                return [MicrostructureResponse(**dict(r)) for r in rows]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))