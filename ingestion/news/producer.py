import hashlib
import logging
import time
from typing import Any

import feedparser

from ingestion.common.kafka_producer import send
from ingestion.common.redis_dedup import add_to_bloom, is_duplicate
from ingestion.common.schemas import normalize_event
from utils.env import get_env
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


def _entry_id(entry: dict[str, Any]) -> str:
    raw = (
        entry.get("id")
        or entry.get("link")
        or entry.get("title")
        or str(time.time_ns())
    )
    return hashlib.sha1(str(raw).encode("utf-8")).hexdigest()


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