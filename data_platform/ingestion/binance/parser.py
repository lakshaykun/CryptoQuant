# data_platform/ingestion/binance/parser.py

def parse_historical_kline(row, symbol: str):
    """
    Parses a single row from Binance REST API /v3/klines response.
    Adds the symbol since it's not present in the raw row but needed downstream.
    """
    return {
        "symbol": symbol,
        "open_time": row[0],
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": float(row[5]),
        "close_time": row[6],
        "quote_volume": float(row[7]),
        "trades": int(row[8]),
        "taker_buy_base": float(row[9]),
        "taker_buy_quote": float(row[10]),
        "ignore": 0,
    }

def parse_ws_kline(data):
    """
    Extract kline data from WebSocket message into required schema.
    """
    if "data" not in data:
        return None

    d = data["data"]

    # Only process kline events
    if d.get("e") != "kline":
        return None

    k = d["k"]

    return {
        "symbol": d.get("s"),
        "open_time": k["t"],
        "open": float(k["o"]),
        "high": float(k["h"]),
        "low": float(k["l"]),
        "close": float(k["c"]),
        "volume": float(k["v"]),
        "close_time": k["T"],
        "quote_volume": float(k["q"]),
        "trades": int(k["n"]),
        "taker_buy_base": float(k["V"]),
        "taker_buy_quote": float(k["Q"]),
        "is_closed": k["x"],  # useful for filtering final candles
        "ignore": 0
    }
