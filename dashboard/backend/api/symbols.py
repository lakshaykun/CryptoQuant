def to_sentiment_symbol(symbol: str) -> str:
    """
    Convert market pair symbols used by dashboard (e.g. BTCUSDT, ETHUSDT)
    to base symbols used by sentiment tables (e.g. BTC, ETH).
    """
    s = (symbol or "").strip().upper()
    if s.endswith("USDT") and len(s) > 4:
        return s[:-4]
    return s
