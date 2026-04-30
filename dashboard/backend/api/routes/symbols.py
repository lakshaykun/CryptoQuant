from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from datetime import datetime
import asyncpg
from dashboard.backend.db.session import get_conn
from fastapi_cache.decorator import cache

router = APIRouter()

class SymbolInfo(BaseModel):
    symbol     : str
    first_time : datetime
    last_time  : datetime
    total_rows : int

@router.get("/", response_model=list[str])
@cache(expire=60)
async def get_symbols(pool: asyncpg.Pool = Depends(get_conn)):
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT symbol FROM market ORDER BY symbol"
            )
        return [r["symbol"] for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{symbol}/info", response_model=SymbolInfo)
async def get_symbol_info(
    symbol: str,
    pool  : asyncpg.Pool = Depends(get_conn)
):
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    symbol,
                    MIN(open_time) AS first_time,
                    MAX(open_time) AS last_time,
                    COUNT(*)       AS total_rows
                FROM market
                WHERE symbol = $1
                GROUP BY symbol
            """, symbol.upper())

        if not row:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

        return SymbolInfo(**dict(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))