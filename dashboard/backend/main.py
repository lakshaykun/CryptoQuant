from fastapi import FastAPI
from contextlib import asynccontextmanager
from dashboard.backend.db.session import init_pool, close_pool
from dashboard.backend.api.routes import candles, features, analytics, portfolio, symbols
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend

@asynccontextmanager
async def lifespan(app: FastAPI):
    FastAPICache.init(InMemoryBackend())
    await init_pool()
    yield
    await close_pool()

app = FastAPI(
    title="CryptoQuant Dashboard API",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(candles.router,   prefix="/api/v1/candles",   tags=["candles"])
app.include_router(features.router,  prefix="/api/v1/features",  tags=["features"])
# app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
# app.include_router(portfolio.router, prefix="/api/v1/portfolio", tags=["portfolio"])
app.include_router(symbols.router,   prefix="/api/v1/symbols",   tags=["symbols"])