from fastapi import APIRouter, Depends, HTTPException, status
import asyncpg
from typing import List
from uuid import UUID
import uuid
from fastapi_cache.decorator import cache

from dashboard.backend.portfolio_db.session import get_portfolio_conn
from dashboard.backend.db.session import get_conn
from dashboard.backend.schemas.portfolio import (
    PortfolioCreate, PortfolioResponse, PortfolioDetail,
    PositionCreate, PositionUpdate, PositionResponse, PositionWithPnL,
    RiskItem, RiskResponse, ScoreItem, SuggestionItem
)
router = APIRouter()

# ----------------- PORTFOLIO CRUD -----------------

@router.post("/", response_model=PortfolioResponse)
async def create_portfolio(
    pf: PortfolioCreate,
    pool: asyncpg.Pool = Depends(get_portfolio_conn)
):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO portfolios (id, name)
            VALUES ($1, $2)
            RETURNING id, name, created_at
            """,
            uuid.uuid4(), pf.name
        )
        return {"id": row["id"], "name": row["name"], "created_at": row["created_at"], "position_count": 0}

@router.get("/", response_model=List[PortfolioResponse])
async def get_portfolios(
    pool: asyncpg.Pool = Depends(get_portfolio_conn)
):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.id, p.name, p.created_at, COUNT(pos.id) as position_count
            FROM portfolios p
            LEFT JOIN positions pos ON pos.portfolio_id = p.id
            GROUP BY p.id
            ORDER BY p.created_at DESC
            """
        )
        return [dict(r) for r in rows]

@router.get("/{id}", response_model=PortfolioDetail)
async def get_portfolio(
    id: UUID,
    p_pool: asyncpg.Pool = Depends(get_portfolio_conn),
    m_pool: asyncpg.Pool = Depends(get_conn)
):
    async with p_pool.acquire() as p_conn:
        pf_row = await p_conn.fetchrow("SELECT id, name, created_at FROM portfolios WHERE id = $1", id)
        if not pf_row:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        
        pos_rows = await p_conn.fetch("SELECT symbol, quantity, buy_price, opened_at FROM positions WHERE portfolio_id = $1", id)
        
    symbols = [r["symbol"] for r in pos_rows]
    market_data = {}
    if symbols:
        async with m_pool.acquire() as m_conn:
            m_rows = await m_conn.fetch(
                """
                SELECT DISTINCT ON (symbol) symbol, close
                FROM market
                WHERE is_valid_feature_row = TRUE AND symbol = ANY($1)
                ORDER BY symbol, open_time DESC
                """,
                symbols
            )
            market_data = {r["symbol"]: r["close"] for r in m_rows}

    positions = []
    total_val = 0.0
    total_pnl = 0.0
    
    for r in pos_rows:
        sym = r["symbol"]
        qty = r["quantity"]
        bp = r["buy_price"]
        current_price = market_data.get(sym)
        pnl_pct = None
        pnl_value = None
        current_value = None
        
        if current_price is not None:
            if bp > 0:
                pnl_pct = (current_price - bp) / bp * 100
            pnl_value = (current_price - bp) * qty
            current_value = current_price * qty
            total_val += current_value
            total_pnl += pnl_value
            
        positions.append({
            "symbol": sym,
            "quantity": qty,
            "buy_price": bp,
            "opened_at": r["opened_at"],
            "current_price": current_price,
            "pnl_pct": pnl_pct,
            "pnl_value": pnl_value,
            "current_value": current_value
        })

    return {
        "id": pf_row["id"],
        "name": pf_row["name"],
        "created_at": pf_row["created_at"],
        "positions": positions,
        "total_value": total_val if market_data else None,
        "total_pnl_value": total_pnl if market_data else None,
    }

@router.put("/{id}", response_model=PortfolioResponse)
async def update_portfolio(
    id: UUID,
    pf: PortfolioCreate,
    pool: asyncpg.Pool = Depends(get_portfolio_conn)
):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE portfolios SET name = $1 WHERE id = $2
            RETURNING id, name, created_at
            """,
            pf.name, id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        count = await conn.fetchval("SELECT COUNT(*) FROM positions WHERE portfolio_id = $1", id)
        return {"id": row["id"], "name": row["name"], "created_at": row["created_at"], "position_count": count}

@router.delete("/{id}")
async def delete_portfolio(
    id: UUID,
    pool: asyncpg.Pool = Depends(get_portfolio_conn)
):
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM portfolios WHERE id = $1", id)
        if res == "DELETE 0":
            raise HTTPException(status_code=404, detail="Portfolio not found")
        return {"ok": True}

# ----------------- POSITIONS CRUD -----------------

@router.post("/{id}/positions", response_model=PositionResponse)
async def add_position(
    id: UUID,
    pos: PositionCreate,
    pool: asyncpg.Pool = Depends(get_portfolio_conn)
):
    async with pool.acquire() as conn:
        # Verify portfolio
        if not await conn.fetchval("SELECT 1 FROM portfolios WHERE id = $1", id):
            raise HTTPException(status_code=404, detail="Portfolio not found")
            
        row = await conn.fetchrow(
            """
            INSERT INTO positions (id, portfolio_id, symbol, quantity, buy_price)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (portfolio_id, symbol) DO NOTHING
            RETURNING symbol, quantity, buy_price, opened_at
            """,
            uuid.uuid4(), id, pos.symbol, pos.quantity, pos.buy_price
        )
        if not row:
            raise HTTPException(status_code=400, detail="Position already exists or invalid data")
        return dict(row)

@router.put("/{id}/positions/{symbol}", response_model=PositionResponse)
async def update_position(
    id: UUID,
    symbol: str,
    pos: PositionUpdate,
    pool: asyncpg.Pool = Depends(get_portfolio_conn)
):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE positions SET quantity = $1, buy_price = $2
            WHERE portfolio_id = $3 AND symbol = $4
            RETURNING symbol, quantity, buy_price, opened_at
            """,
            pos.quantity, pos.buy_price, id, symbol
        )
        if not row:
            raise HTTPException(status_code=404, detail="Position not found")
        return dict(row)

@router.delete("/{id}/positions/{symbol}")
async def delete_position(
    id: UUID,
    symbol: str,
    pool: asyncpg.Pool = Depends(get_portfolio_conn)
):
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM positions WHERE portfolio_id = $1 AND symbol = $2", id, symbol)
        if res == "DELETE 0":
            raise HTTPException(status_code=404, detail="Position not found")
        return {"ok": True}

# ----------------- ANALYTICS -----------------

@router.get("/{id}/risk", response_model=RiskResponse)
@cache(expire=60)
async def get_risk(
    id: UUID,
    p_pool: asyncpg.Pool = Depends(get_portfolio_conn),
    m_pool: asyncpg.Pool = Depends(get_conn)
):
    async with p_pool.acquire() as p_conn:
        pos_rows = await p_conn.fetch("SELECT symbol, quantity FROM positions WHERE portfolio_id = $1", id)
        
    if not pos_rows:
        return {"positions": [], "total_portfolio_risk": 0.0}
        
    symbols = [r["symbol"] for r in pos_rows]
    qty_map = {r["symbol"]: r["quantity"] for r in pos_rows}
    
    async with m_pool.acquire() as m_conn:
        m_rows = await m_conn.fetch(
            """
            SELECT DISTINCT ON (symbol) symbol, close, volatility_5
            FROM market
            WHERE is_valid_feature_row = TRUE AND symbol = ANY($1)
            ORDER BY symbol, open_time DESC
            """,
            symbols
        )
        
    pos_details = []
    total_port_value = 0.0
    for r in m_rows:
        sym = r["symbol"]
        qty = qty_map[sym]
        pval = r["close"] * qty if r["close"] else 0.0
        total_port_value += pval
        pos_details.append({
            "symbol": sym,
            "volatility_5": r["volatility_5"],
            "position_value": pval
        })
        
    risk_items = []
    total_risk = 0.0
    for pd in pos_details:
        w_risk = 0.0
        if total_port_value > 0 and pd["volatility_5"]:
            w_risk = pd["volatility_5"] * (pd["position_value"] / total_port_value)
            total_risk += w_risk
            
        risk_items.append({
            "symbol": pd["symbol"],
            "volatility_5": pd["volatility_5"],
            "position_value": pd["position_value"],
            "weighted_risk": w_risk
        })
        
    return {"positions": risk_items, "total_portfolio_risk": total_risk}

@router.get("/{id}/scores", response_model=List[ScoreItem])
@cache(expire=60)
async def get_scores(
    id: UUID,
    p_pool: asyncpg.Pool = Depends(get_portfolio_conn),
    m_pool: asyncpg.Pool = Depends(get_conn)
):
    async with p_pool.acquire() as p_conn:
        pos_rows = await p_conn.fetch("SELECT symbol FROM positions WHERE portfolio_id = $1", id)
    in_port = {r["symbol"] for r in pos_rows}
    
    async with m_pool.acquire() as m_conn:
        rows = await m_conn.fetch("""
        WITH latest_market AS (
            SELECT DISTINCT ON (symbol) symbol, volatility_5
            FROM market
            WHERE is_valid_feature_row = TRUE
            ORDER BY symbol, open_time DESC
        ),
        latest_predictions AS (
            SELECT DISTINCT ON (symbol) symbol, prediction AS predicted_log_return
            FROM predictions
            ORDER BY symbol, open_time DESC
        ),
        latest_sentiment AS (
            SELECT DISTINCT ON (symbol) symbol, sentiment_index
            FROM sentiment_gold
            ORDER BY symbol, window_start DESC
        )
        SELECT 
            m.symbol,
            p.predicted_log_return,
            m.volatility_5,
            COALESCE(s.sentiment_index, 1.0) AS sentiment_index
        FROM latest_market m
        JOIN latest_predictions p ON m.symbol = p.symbol
        LEFT JOIN latest_sentiment s ON UPPER(s.symbol) = UPPER(REPLACE(m.symbol, 'USDT', ''))
        """)

    scores = []
    for r in rows:
        plr = r["predicted_log_return"]
        vol = r["volatility_5"]
        si = r["sentiment_index"]
        
        score = None
        if plr is not None and vol is not None and si is not None and vol != 0:
            score = (plr / vol) * si
            
        scores.append({
            "symbol": r["symbol"],
            "predicted_log_return": plr,
            "volatility_5": vol,
            "sentiment_index": si,
            "score": score,
            "in_portfolio": r["symbol"] in in_port
        })
        
    scores.sort(key=lambda x: x["score"] if x["score"] is not None else -9999, reverse=True)
    return scores

@router.get("/{id}/suggestions", response_model=List[SuggestionItem])
@cache(expire=60)
async def get_suggestions(
    id: UUID,
    p_pool: asyncpg.Pool = Depends(get_portfolio_conn),
    m_pool: asyncpg.Pool = Depends(get_conn)
):
    # Retrieve portfolio state
    async with p_pool.acquire() as p_conn:
        pos_rows = await p_conn.fetch("SELECT symbol, quantity FROM positions WHERE portfolio_id = $1", id)
    
    qty_map = {r["symbol"]: r["quantity"] for r in pos_rows}
    
    async with m_pool.acquire() as m_conn:
        # Get market closely related for current value
        m_rows = await m_conn.fetch("""
        WITH latest_market AS (
            SELECT DISTINCT ON (symbol) symbol, volatility_5, close
            FROM market
            WHERE is_valid_feature_row = TRUE
            ORDER BY symbol, open_time DESC
        ),
        latest_predictions AS (
            SELECT DISTINCT ON (symbol) symbol, prediction AS predicted_log_return
            FROM predictions
            ORDER BY symbol, open_time DESC
        ),
        latest_sentiment AS (
            SELECT DISTINCT ON (symbol) symbol, sentiment_index
            FROM sentiment_gold
            ORDER BY symbol, window_start DESC
        )
        SELECT 
            m.symbol,
            m.close,
            p.predicted_log_return,
            m.volatility_5,
            COALESCE(s.sentiment_index, 1.0) AS sentiment_index
        FROM latest_market m
        JOIN latest_predictions p ON m.symbol = p.symbol
        LEFT JOIN latest_sentiment s ON UPPER(s.symbol) = UPPER(REPLACE(m.symbol, 'USDT', ''))
        """)

    # Compute current weights
    total_val = 0.0
    val_map = {}
    for r in m_rows:
        sym = r["symbol"]
        qty = qty_map.get(sym, 0)
        val = (r["close"] or 0) * qty
        val_map[sym] = val
        total_val += val
        
    # Compute scores for positive elements
    positive_scores = {}
    for r in m_rows:
        plr = r["predicted_log_return"]
        vol = r["volatility_5"]
        si = r["sentiment_index"]
        if plr is not None and vol is not None and si is not None and vol != 0:
            score = (plr / vol) * si
            if score > 0:
                positive_scores[r["symbol"]] = score
                
    total_score = sum(positive_scores.values())
    
    suggestions = []
    for r in m_rows:
        sym = r["symbol"]
        if sym not in positive_scores and sym not in qty_map:
            continue
            
        score = positive_scores.get(sym)
        
        suggested_weight_pct = 0.0
        if score is not None and total_score > 0:
            suggested_weight_pct = (score / total_score) * 100
            
        current_weight_pct = 0.0
        if total_val > 0:
            current_weight_pct = (val_map.get(sym, 0) / total_val) * 100
            
        action = "hold"
        if sym not in qty_map and suggested_weight_pct > 0:
            action = "buy"
        elif sym in qty_map:
            if abs(suggested_weight_pct - current_weight_pct) <= 5.0:
                action = "hold"
            elif suggested_weight_pct > current_weight_pct:
                action = "increase"
            else:
                action = "reduce"
                
        if sym in positive_scores or sym in qty_map:
            suggestions.append({
                "symbol": sym,
                "score": score,
                "suggested_weight_pct": suggested_weight_pct,
                "current_weight_pct": current_weight_pct,
                "action": action
            })
            
    return suggestions
