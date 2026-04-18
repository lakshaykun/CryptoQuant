import time

import requests

from pipelines.ingestion.streaming.sources.sentiment.shared.normalization import (
    normalize_event,
)
from utils.number_utils import percent, to_float, to_int
from utils.source_config import get_list_value, get_sources_section

HEADERS = {"User-Agent": "btc-sentiment-ingestion/1.0"}


def _reddit_engagement_weights(section: dict) -> dict[str, float]:
    raw = section.get("engagement_weights")
    if not isinstance(raw, dict):
        return {}

    parsed: dict[str, float] = {}
    for key in ("score", "upvote_ratio", "comments"):
        try:
            parsed[key] = max(0.0, float(raw.get(key, 0.0)))
        except (TypeError, ValueError):
            parsed[key] = 0.0

    total = sum(parsed.values())
    if total <= 0:
        return {}

    return {key: value / total for key, value in parsed.items()}


def _engagement_score(post: dict, total_score: int, total_comments: int, weights: dict[str, float]) -> int:
    score_pct = percent(to_int(post.get("score", 0)), total_score)
    comments_pct = percent(to_int(post.get("num_comments", 0)), total_comments)
    upvote_pct = max(0.0, min(to_float(post.get("upvote_ratio", 0.0)) * 100.0, 100.0))

    engagement = (
        (weights.get("score", 0.0) * score_pct)
        + (weights.get("upvote_ratio", 0.0) * upvote_pct)
        + (weights.get("comments", 0.0) * comments_pct)
    )
    return max(0, int(round(engagement)))


def fetch_reddit_events() -> list[dict]:
    section = get_sources_section("reddit")
    subreddits = get_list_value(section, "subreddits", [])
    keywords = get_list_value(section, "keywords", [])

    if not subreddits or not keywords:
        return []

    weights = _reddit_engagement_weights(section)

    events: list[dict] = []

    for sub in subreddits:
        for keyword in keywords:
            url = f"https://www.reddit.com/r/{sub}/search.json"
            params = {
                "q": keyword,
                "restrict_sr": "on",
                "sort": "new",
                "t": "day",
                "limit": 100,
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
                    payload = {
                        "id": post.get("id", ""),
                        "timestamp": str(post.get("created_utc", "")),
                        "source": "reddit",
                        "text": f"{post.get('title', '')} {post.get('selftext', '')}",
                        "engagement": _engagement_score(post, total_score, total_comments, weights),
                        "symbol": "BTC",
                    }
                    try:
                        events.append(normalize_event(payload))
                    except ValueError:
                        continue

                time.sleep(1)
            except Exception:
                continue

    return events
