import json

async def send_ws_safe(websocket, data):
    try:
        await websocket.send_text(json.dumps(data))
    except Exception as e:
        print(f"[WS] Send error: {e}")