# pipelines/ingestion/batch/sources/market/binance_today.py

from datetime import datetime, timezone
import time

import pandas as pd
import requests

from pipelines.storage.local.csv import (
    build_filename,
    build_path,
    ensure_dir,
    file_exists,
    save_csv,
)

SOURCE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]

RAW_MARKET_COLUMNS = [
    "symbol", "open_time", "close_time", "open", "high", "low", "close",
    "volume", "quote_volume", "trades", "taker_buy_base", "taker_buy_quote",
]

REQUEST_TIMEOUT_SECONDS = 600
MAX_RETRY_ATTEMPTS = 10
INITIAL_RETRY_DELAY_SECONDS = 10
MAX_RETRY_DELAY_SECONDS = 600


def _request_today(symbol: str, interval: str, render_uri: str, logger):
    retry_delay = INITIAL_RETRY_DELAY_SECONDS
    last_error = None

    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(
                render_uri,
                params={"symbol": symbol, "interval": interval},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc

            if attempt == MAX_RETRY_ATTEMPTS:
                break

            logger.warning(
                f"⚠️ Request failed for {symbol} at {interval} "
                f"[attempt {attempt}/{MAX_RETRY_ATTEMPTS}]. "
                f"Retrying in {retry_delay}s: {exc}"
            )
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY_SECONDS)

    if last_error is not None:
        raise last_error

    raise RuntimeError(f"Failed to fetch data for {symbol} at {interval}.")


def fetch_today(symbol: str, interval: str, base_path: str, render_uri: str, logger):
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    filename = build_filename(symbol, interval, date_str)
    file_path = build_path(base_path, filename)

    ensure_dir(base_path)

    if file_exists(file_path):
        logger.info(f"⏭️ Skipped: {filename}")
        return

    try:
        payload = _request_today(symbol, interval, render_uri, logger)
        rows = payload.get("data", []) if isinstance(payload, dict) else payload

        if not rows:
            logger.warning(f"⚠️ No data returned for: {filename}")
            return

        df = pd.DataFrame(rows)

        if list(df.columns) == SOURCE_COLUMNS:
            df = df.copy()
        elif "ignore" not in df.columns:
            df["ignore"] = 0

        if "symbol" not in df.columns:
            df["symbol"] = symbol.upper()

        df = df[RAW_MARKET_COLUMNS]
        save_csv(df, file_path)

        logger.info(f"💾 Saved: {filename}")

    except Exception as e:
        logger.error(f"⚠️ Error: {filename} → {e}")