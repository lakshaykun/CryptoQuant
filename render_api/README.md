# CryptoQuant Render Producer

The Render API is the public WebSocket proxy for Binance market data. It exposes a backfill endpoint for the current trading day and a live endpoint that forwards closed Binance kline events to downstream consumers.

## Endpoints

- `GET /` - health check.
- `WS /ws/binance/backfill` - sends the current day's historical candles by symbol.
- `WS /ws/binance/live` - streams live closed candles with heartbeat messages.

## Key files

- `app/main.py` - FastAPI app and WebSocket routes.
- `app/binance_ws.py` - Binance socket connection and candle parsing.
- `app/historical.py` - same-day historical candle fetcher.
- `app/config.py` - environment-backed defaults for symbols and stream type.
- `app/utils.py` - WebSocket send helper.

## Role in the system

This service is the upstream data source for the streaming pipeline in `pipelines/ingestion/streaming/`. It should stay focused on transport, parsing, and retry behavior rather than storage or model logic.

## Deployment notes

- The app can run locally with `uvicorn app.main:app --reload`.
- The Render start command should bind to `0.0.0.0` and the platform-provided port.
- Keep the symbol and interval defaults aligned with `configs/data.yaml`.

## Future direction

- Add stronger monitoring for reconnects and WebSocket disconnects.
- Make the stream contract explicit if additional exchanges or instruments are added.