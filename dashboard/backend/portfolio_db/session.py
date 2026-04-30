import os
import asyncpg
from typing import AsyncGenerator

# Use asyncpg connection pooling setup
PORTFOLIO_POOL = None

async def init_portfolio_pool():
    global PORTFOLIO_POOL
    PORTFOLIO_DATABASE_URL = os.environ.get("PORTFOLIO_DATABASE_URL")
    if not PORTFOLIO_DATABASE_URL:
        # Fallback for dev if needed, or raise
        PORTFOLIO_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/portfolio_db"
    
    PORTFOLIO_POOL = await asyncpg.create_pool(PORTFOLIO_DATABASE_URL)

async def close_portfolio_pool():
    global PORTFOLIO_POOL
    if PORTFOLIO_POOL:
        await PORTFOLIO_POOL.close()

async def get_portfolio_conn() -> asyncpg.Pool:
    global PORTFOLIO_POOL
    if not PORTFOLIO_POOL:
        raise Exception("PORTFOLIO_POOL is not initialized")
    return PORTFOLIO_POOL
