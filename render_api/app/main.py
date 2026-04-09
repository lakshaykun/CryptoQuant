# render_api/app/main.py
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Query
import asyncio

from .binance_ws import stream_binance
from .utils import send_ws_safe
from .config import DEFAULT_SYMBOLS

app = FastAPI(title="CryptoQuant Render Producer")


@app.api_route("/", methods=["GET", "HEAD"])
def health(request: Request):
    return {"status": "ok"}


VALID_INTERVALS = {
    "1m","3m","5m","15m","30m",
    "1h","2h","4h","6h","8h","12h",
    "1d","3d","1w","1M"
}


@app.websocket("/ws/binance")
async def websocket_endpoint(
    websocket: WebSocket,
    symbols: str = Query(None),
    interval: str = Query("1m")
):
    await websocket.accept()

    if interval not in VALID_INTERVALS:
        await websocket.send_text(
            f'{{"error": "Invalid interval {interval}"}}'
        )
        await websocket.close()
        return
    
    symbol_list = (
        [s.strip().lower() for s in symbols.split(",")]
        if symbols else DEFAULT_SYMBOLS
    )

    # dynamic interval
    stream = f"kline_{interval}"

    print(f"[Client] Connected | Symbols: {symbol_list} | Interval: {interval}")

    async def sender(data):
        await send_ws_safe(websocket, data)

    task = asyncio.create_task(stream_binance(symbol_list, stream, sender))

    try:
        while True:
            await asyncio.sleep(10)
            await websocket.send_text('{"type":"heartbeat"}')

    except WebSocketDisconnect:
        print("[Client] Disconnected")
        task.cancel()