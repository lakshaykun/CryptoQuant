import hashlib
import logging
import time
from datetime import UTC, datetime
from typing import Any

import feedparser

from ingestion.common.engagement_utils import normalized_weights
from ingestion.common.kafka_producer import send
from ingestion.common.redis_dedup import add_to_bloom, is_duplicate
from ingestion.common.schemas import normalize_event
from utils.env import get_env
from utils.number_utils import percent, to_int
from utils.source_config import get_list_value, get_sources_section


logger = logging.getLogger(__name__)

DEFAULT_FEEDS = [
    "https://bitcoinmagazine.com/.rss/full/",
    "https://news.bitcoin.com/feed/",
]

DEFAULT_SYMBOL = "BTC"
DEFAULT_INTERVAL_SECONDS = 300
MAX_ENTRIES_PER_FEED = 25
KAFKA_TOPIC = "btc_news"
NEWS_SIGNAL_KEYWORDS = [
    "btc", "bitcoin", "etf", "fed", "halving", "breakout", "crash", "inflation", "whale", "liquidation"
]
DEFAULT_NEWS_ENGAGEMENT_WEIGHTS = {
    "comments": 0.45,
    "keywords": 0.25,
    "recency": 0.30,
}


def _configured_feeds() -> list[str]:
    section = get_sources_section("news")

    feeds = get_list_value(section, "feeds", [])
    if feeds:
        return feeds

    raw = get_env("NEWS_FEEDS", default=",".join(DEFAULT_FEEDS))
    env_feeds = [item.strip() for item in raw.split(",") if item.strip()]
    return env_feeds or DEFAULT_FEEDS


def _news_symbol() -> str:
    section = get_sources_section("news")
    symbol = str(section.get("currency", DEFAULT_SYMBOL)).strip()
    return symbol or DEFAULT_SYMBOL


def _news_interval_seconds() -> int:
    section = get_sources_section("news")
    raw = section.get("interval_seconds", DEFAULT_INTERVAL_SECONDS)

    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_INTERVAL_SECONDS


def _news_engagement_weights() -> dict[str, float]:
    section = get_sources_section("news")
    raw = section.get("engagement_weights", {})
    if not isinstance(raw, dict):
        return DEFAULT_NEWS_ENGAGEMENT_WEIGHTS.copy()
    return normalized_weights(raw, DEFAULT_NEWS_ENGAGEMENT_WEIGHTS)


def _entry_id(entry: dict[str, Any]) -> str:
    raw = (
        entry.get("id")
        or entry.get("link")
        or entry.get("title")
        or str(time.time_ns())
    )
    return hashlib.sha1(str(raw).encode("utf-8")).hexdigest()


def _entry_datetime(entry: dict[str, Any]) -> datetime:
    now = datetime.now(UTC)

    for key in ("published_parsed", "updated_parsed"):
        parsed = entry.get(key)
        if parsed:
            try:
                return datetime.fromtimestamp(time.mktime(parsed), tz=UTC)
            except (TypeError, ValueError, OverflowError):
                continue

    for key in ("published", "updated"):
        raw = entry.get(key)
        if not raw:
            continue

        text = str(raw).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            return datetime.fromisoformat(text).astimezone(UTC)
        except ValueError:
            continue

    return now


def _keyword_hits(text: str) -> int:
    lowered = text.lower()
    return sum(1 for token in NEWS_SIGNAL_KEYWORDS if token in lowered)


def _entry_signal(entry: dict[str, Any]) -> dict[str, float]:
    now = datetime.now(UTC)
    published = _entry_datetime(entry)
    age_hours = max(0.0, (now - published).total_seconds() / 3600.0)

    text = f"{entry.get('title', '')} {entry.get('summary', '')}".strip()
    comment_count = to_int(entry.get("slash_comments") or entry.get("comments"))
    recency_points = max(0.0, 100.0 - min(age_hours, 72.0) * (100.0 / 72.0))

    return {
        "comment_count": float(comment_count),
        "keyword_hits": float(_keyword_hits(text)),
        "recency_points": recency_points,
    }


def _build_news_engagement(signals: list[dict[str, float]], index: int, weights: dict[str, float]) -> int:
    signal = signals[index]

    total_comments = sum(item["comment_count"] for item in signals)
    total_keywords = sum(item["keyword_hits"] for item in signals)
    total_recency = sum(item["recency_points"] for item in signals)

    comments_pct = percent(signal["comment_count"], total_comments)
    keywords_pct = percent(signal["keyword_hits"], total_keywords)
    recency_pct = percent(signal["recency_points"], total_recency)

    # Composite score similar to YouTube's multi-signal engagement.
    engagement = (
        (weights["comments"] * comments_pct)
        + (weights["keywords"] * keywords_pct)
        + (weights["recency"] * recency_pct)
    )
    return max(1, int(round(engagement)))


def _build_event(entry: dict[str, Any]) -> dict[str, Any] | None:
    text = f"{entry.get('title', '')} {entry.get('summary', '')}".strip()

    if not text:
        return None

    event = {
        "id": _entry_id(entry),
        "timestamp": entry.get("published")
        or entry.get("updated")
        or time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "news",
        "text": text,
        "engagement": 1,
        "symbol": _news_symbol(),
    }

    try:
        return normalize_event(event)
    except ValueError as e:
        logger.warning("Invalid event skipped: %s", e)
        return None


def fetch_news_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    signals: list[dict[str, float]] = []
    weights = _news_engagement_weights()

    for feed_url in _configured_feeds():
        try:
            parsed = feedparser.parse(feed_url)
        except Exception as e:
            logger.error("Feed parse failed for %s: %s", feed_url, e)
            continue

        if getattr(parsed, "bozo", False):
            logger.warning(
                "Malformed RSS feed %s: %s",
                feed_url,
                parsed.bozo_exception,
            )
            continue

        logger.info(
            "Fetched %d entries from %s",
            len(parsed.entries),
            feed_url,
        )

        for entry in parsed.entries[:MAX_ENTRIES_PER_FEED]:
            event = _build_event(entry)
            if event:
                items.append(event)
                signals.append(_entry_signal(entry))

    for idx, event in enumerate(items):
        event["engagement"] = _build_news_engagement(signals, idx, weights)

    return items


def publish_news() -> int:
    sent = 0
    items = fetch_news_items()

    logger.info("Total normalized items fetched: %d", len(items))

    for item in items:
        item_id = item["id"]

        try:
            if is_duplicate(item_id):
                continue

            send(KAFKA_TOPIC, item)
            add_to_bloom(item_id)
            sent += 1

        except Exception as e:
            logger.error("Failed publishing item %s: %s", item_id, e)

    logger.info("Published %d new items", sent)
    return sent


def run_forever(interval_seconds: int | None = None):
    interval = interval_seconds or _news_interval_seconds()

    logger.info("Starting news producer with %d second interval", interval)

    while True:
        try:
            publish_news()
        except Exception as e:
            logger.exception("Producer loop failed: %s", e)

        time.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_forever()