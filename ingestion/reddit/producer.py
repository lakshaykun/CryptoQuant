import requests
import time
from ingestion.common.engagement_utils import normalized_weights
from ingestion.common.kafka_producer import send
from ingestion.common.schemas import normalize_event
from utils.number_utils import percent, to_float, to_int
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

DEFAULT_REDDIT_ENGAGEMENT_WEIGHTS = {
    "score": 0.45,
    "upvote_ratio": 0.35,
    "comments": 0.20,
}

def _reddit_engagement_weights(section: dict) -> dict[str, float]:
    raw = section.get("engagement_weights", {})
    if not isinstance(raw, dict):
        return DEFAULT_REDDIT_ENGAGEMENT_WEIGHTS.copy()
    return normalized_weights(raw, DEFAULT_REDDIT_ENGAGEMENT_WEIGHTS)


def _engagement_score(post: dict, total_score: int, total_comments: int, weights: dict[str, float]) -> int:
    # Mirror the YouTube approach with a composite percentage score.
    score_pct = percent(to_int(post.get("score", 0)), total_score)
    comments_pct = percent(to_int(post.get("num_comments", 0)), total_comments)
    upvote_pct = max(0.0, min(to_float(post.get("upvote_ratio", 0.0)) * 100.0, 100.0))

    # Weighting: upvotes and score matter most, comments provide depth signal.
    engagement = (
        (weights["score"] * score_pct)
        + (weights["upvote_ratio"] * upvote_pct)
        + (weights["comments"] * comments_pct)
    )
    return max(0, int(round(engagement)))

def fetch_reddit():
    """
    Scrapes Reddit using the JSON API while maintaining 
    the original Kafka ingestion flow.
    """
    section = get_sources_section("reddit")
    subreddits = get_list_value(section, "subreddits", DEFAULT_SUBREDDITS)
    keywords = get_list_value(section, "keywords", DEFAULT_KEYWORDS)
    weights = _reddit_engagement_weights(section)

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
                post_data = [obj.get("data", {}) for obj in posts if isinstance(obj, dict)]

                total_score = sum(to_int(post.get("score", 0)) for post in post_data)
                total_comments = sum(to_int(post.get("num_comments", 0)) for post in post_data)

                for post in post_data:
                    post_id = post["id"]

                    payload = {
                        "id": post_id,
                        "timestamp": str(post["created_utc"]),
                        "source": "reddit",
                        "text": f"{post['title']} {post.get('selftext', '')}",
                        "engagement": _engagement_score(post, total_score, total_comments, weights),
                        "symbol": "BTC"
                    }
                    try:
                        payload = normalize_event(payload)
                    except ValueError:
                        continue
                    
                    send("btc_reddit", payload)

                # Rate limiting to be polite to the API
                time.sleep(1)

            except Exception as e:
                print(f"Error fetching from r/{sub}: {e}")
                continue

def run_forever():
    while True:
        fetch_reddit()
        print("pulling messages from reddit...")
        time.sleep(300)


if __name__ == "__main__":
    run_forever()