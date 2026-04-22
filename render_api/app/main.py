from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Query, HTTPException
import asyncio

from .binance_ws import stream_binance
from .utils import send_ws_safe
from .config import DEFAULT_SYMBOLS
from .historical import fetch_today_klines

app = FastAPI(title="CryptoQuant Render Producer")


@app.api_route("/", methods=["GET", "HEAD"])
def health(request: Request):
    return {"status": "ok"}


@app.get("/binance/backfill")
def market_backfill(
    symbol: str = Query(...),
    interval: str = Query("1m")
):
    VALID_INTERVALS = {
        "1m","3m","5m","15m","30m",
        "1h","2h","4h","6h","8h","12h",
        "1d","3d","1w","1M"
    }

    if "," in symbol:
        raise HTTPException(status_code=400, detail="Only one symbol is allowed")

    symbol = symbol.strip().lower()

    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")

    if interval not in VALID_INTERVALS:
        raise HTTPException(status_code=400, detail=f"Invalid interval {interval}")

    print(f"[Backfill] Symbol: {symbol} | Interval: {interval}")

    data = fetch_today_klines(symbol, interval)

    return {
        "type": "backfill_complete",
        "symbol": symbol.upper(),
        "interval": interval,
        "count": len(data),
        "data": data,
    }

@app.websocket("/ws/binance/live")
async def websocket_live(
    websocket: WebSocket,
    symbols: str = Query(None),
    interval: str = Query("1m")
):
    await websocket.accept()

    symbol_list = (
        [s.strip().lower() for s in symbols.split(",")]
        if symbols else DEFAULT_SYMBOLS
    )

    VALID_INTERVALS = {
        "1m","3m","5m","15m","30m",
        "1h","2h","4h","6h","8h","12h",
        "1d","3d","1w","1M"
    }

    if interval not in VALID_INTERVALS:
        await websocket.send_text(f'{{"error": "Invalid interval {interval}"}}')
        await websocket.close()
        return

    stream = f"kline_{interval}"

    print(f"[Live] Symbols: {symbol_list} | Interval: {interval}")

    async def sender(data):
        await send_ws_safe(websocket, data)

    task = asyncio.create_task(stream_binance(symbol_list, stream, sender))

    try:
        while True:
            await asyncio.sleep(10)
            await websocket.send_text('{"type":"heartbeat"}')

    except WebSocketDisconnect:
        print("[Live] Client disconnected")
        task.cancel()

    except Exception as e:
        print(f"[Live Error]: {e}")
        task.cancel()