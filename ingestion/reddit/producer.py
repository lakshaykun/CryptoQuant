import requests
import time
from ingestion.common.redis_dedup import is_duplicate, add_to_bloom
from ingestion.common.kafka_producer import send
from utils.source_config import get_list_value, get_sources_section

# Expanded subreddit list
DEFAULT_SUBREDDITS = [
    "Bitcoin", "CryptoCurrency", "btc", "CryptoMarkets", 
    "BitcoinMarkets", "solana", "ethstaker", "ethereum", 
    "Altcoin", "WallStreetBetsCrypto", "Binance"
]

DEFAULT_KEYWORDS = ["BTC", "Bitcoin", "BTCUSD", "breakout", "crash"]

HEADERS = {
    "User-Agent": "btc-sentiment-ingestion/1.0"
}

def fetch_reddit():
    """
    Scrapes Reddit using the JSON API while maintaining 
    the original Kafka/Redis ingestion flow.
    """
    section = get_sources_section("reddit")
    subreddits = get_list_value(section, "subreddits", DEFAULT_SUBREDDITS)
    keywords = get_list_value(section, "keywords", DEFAULT_KEYWORDS)

    for sub in subreddits:
        for keyword in keywords:
            url = f"https://www.reddit.com/r/{sub}/search.json"
            params = {
                "q": keyword,
                "restrict_sr": "on",
                "sort": "new",
                "t": "day",
                "limit": 100
            }

            try:
                response = requests.get(url, headers=HEADERS, params=params, timeout=20)
                
                if response.status_code != 200:
                    continue

                data = response.json()
                posts = data.get("data", {}).get("children", [])

                for post_obj in posts:
                    post = post_obj["data"]
                    post_id = post["id"]

                    # Original Deduplication Logic
                    if is_duplicate(post_id):
                        continue

                    # Original Payload Format
                    payload = {
                        "id": post_id,
                        "timestamp": str(post["created_utc"]),
                        "source": "reddit",
                        "text": f"{post['title']} {post.get('selftext', '')}",
                        "engagement": post["score"],
                        "symbol": "BTC"
                    }
                    print(payload)  # For debugging
                    

                    # Original Ingestion Logic
                    send("btc_reddit", payload)
                    add_to_bloom(post_id)

                # Rate limiting to be polite to the API
                time.sleep(1)

            except Exception as e:
                print(f"Error fetching from r/{sub}: {e}")
                continue

def run_forever():
    while True:
        fetch_reddit()
        print("pulling messages from youtube...")
        time.sleep(300)


if __name__ == "__main__":
    run_forever()