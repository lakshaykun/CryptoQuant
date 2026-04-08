# CryptoQuant Render Producer

WebSocket proxy for Binance streams.

## Endpoint

/ws/binance?symbols=btcusdt,ethusdt&stream_type=trade

## Run locally

uvicorn app.main:app --reload

## Deploy on Render

- Root directory: render_producer
- Build: pip install -r requirements.txt
- Start: uvicorn app.main:app --host 0.0.0.0 --port 10000