import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

_pool: asyncpg.Pool | None = None


async def init_pool():
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=os.getenv("DATABASE_URL_LOCAL"),
        min_size=2,
        max_size=10
    )


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()


async def get_conn() -> asyncpg.Connection:
    return _pool