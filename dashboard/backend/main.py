from fastapi import FastAPI
from contextlib import asynccontextmanager
from dashboard.backend.db.session import init_pool, close_pool
from dashboard.backend.portfolio_db.session import init_portfolio_pool, close_portfolio_pool
from dashboard.backend.api.routes import candles, features, analytics, portfolio, symbols, predictions, sentiment
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend

@asynccontextmanager
async def lifespan(app: FastAPI):
    FastAPICache.init(InMemoryBackend())
    await init_pool()
    await init_portfolio_pool()
    yield
    await close_pool()
    await close_portfolio_pool()

app = FastAPI(
    title="CryptoQuant Dashboard API",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(candles.router,   prefix="/api/v1/candles",   tags=["candles"])
app.include_router(features.router,  prefix="/api/v1/features",  tags=["features"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
app.include_router(portfolio.router, prefix="/api/v1/portfolio", tags=["portfolio"])
app.include_router(symbols.router,   prefix="/api/v1/symbols",   tags=["symbols"])
app.include_router(predictions.router, prefix="/api/v1/predictions", tags=["predictions"])
app.include_router(sentiment.router, prefix="/api/v1/sentiment", tags=["sentiment"])