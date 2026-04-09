# render_api/app/utils.py

import json
from fastapi import WebSocket


async def send_ws_safe(websocket: WebSocket, data):
    try:
        await websocket.send_text(json.dumps(data))
    except Exception as e:
        print(f"[WS] Send error: {e}")
        raise  # important: propagate disconnect