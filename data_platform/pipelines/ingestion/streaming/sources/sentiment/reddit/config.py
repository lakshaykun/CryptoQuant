DEFAULT_SUBREDDITS = [
    "Bitcoin",
    "CryptoCurrency",
    "btc",
    "CryptoMarkets",
    "BitcoinMarkets",
    "solana",
    "ethstaker",
    "ethereum",
    "Altcoin",
    "WallStreetBetsCrypto",
    "Binance",
]

DEFAULT_KEYWORDS = ["BTC", "Bitcoin", "BTCUSD", "breakout", "crash"]
HEADERS = {"User-Agent": "btc-sentiment-ingestion/1.0"}

DEFAULT_REDDIT_ENGAGEMENT_WEIGHTS = {
    "score": 0.45,
    "upvote_ratio": 0.35,
    "comments": 0.20,
}
