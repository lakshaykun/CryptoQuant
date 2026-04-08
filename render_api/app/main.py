from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
import asyncio

from .binance_ws import stream_binance
from .utils import send_ws_safe
from .config import DEFAULT_SYMBOLS, DEFAULT_STREAM

app = FastAPI(title="CryptoQuant Render Producer")


@app.get("/")
def health():
    return {"status": "ok"}


@app.websocket("/ws/binance")
async def websocket_endpoint(
    websocket: WebSocket,
    symbols: str = Query(None),
    stream_type: str = Query(None)
):
    await websocket.accept()

    symbol_list = (
        [s.strip().lower() for s in symbols.split(",")]
        if symbols else DEFAULT_SYMBOLS
    )

    stream = stream_type if stream_type else DEFAULT_STREAM

    print(f"[Client] Connected | Symbols: {symbol_list} | Stream: {stream}")

    async def sender(data):
        await send_ws_safe(websocket, data)

    task = asyncio.create_task(stream_binance(symbol_list, stream, sender))

    try:
        while True:
            # keep connection alive
            await websocket.receive_text()

    except WebSocketDisconnect:
        print("[Client] Disconnected")
        task.cancel()