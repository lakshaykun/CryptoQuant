import logging
import time

import requests

from pipelines.ingestion.streaming.sources.sentiment.shared.normalization import (
    normalize_event,
)
from pipelines.ingestion.streaming.sources.sentiment.shared.ingestion_state import apply_ingestion_policies
from utils.number_utils import percent, to_float, to_int
from utils.source_config import get_list_value, get_sources_section, load_sources_config

HEADERS = {"User-Agent": "crypto-sentiment-ingestion/1.0"}

logger = logging.getLogger(__name__)

DEFAULT_REQUEST_TIMEOUT = (3.05, 10)

# ─── Symbols ────────────────────────────────────────────────────────────────

def _configured_symbols() -> list[str]:
    """Return the list of crypto symbols to track (e.g. BTC, ETH, SOL, BNB, XRP)."""
    cfg = load_sources_config()
    raw = cfg.get("symbols", ["BTC"])
    if isinstance(raw, list):
        return [str(s).strip().upper() for s in raw if str(s).strip()]
    return ["BTC"]


# ─── Per-symbol subreddits and keywords ─────────────────────────────────────

def _subreddits_for_symbol(section: dict, symbol: str) -> list[str]:
    """Return subreddits dedicated to *symbol* from sources.yaml."""
    by_sym = section.get("subreddits_by_symbol", {})
    raw = by_sym.get(symbol, [])
    if isinstance(raw, list):
        return [str(s).strip() for s in raw if str(s).strip()]
    # Fallback: legacy flat list if the new key is absent
    return get_list_value(section, "subreddits", [])


def _keywords_for_symbol(section: dict, symbol: str) -> list[str]:
    """Return search keywords dedicated to *symbol* from sources.yaml."""
    by_sym = section.get("keywords_by_symbol", {})
    raw = by_sym.get(symbol, [])
    if isinstance(raw, list):
        return [str(k).strip() for k in raw if str(k).strip()]
    # Fallback: legacy flat list
    return get_list_value(section, "keywords", [])


def _combined_reddit_query(keywords: list[str]) -> str:
    cleaned: list[str] = []
    for kw in keywords:
        token = str(kw).strip()
        if not token:
            continue
        if " " in token and not (token.startswith('"') and token.endswith('"')):
            token = f'"{token}"'
        cleaned.append(token)

    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    return "(" + " OR ".join(cleaned) + ")"


# ─── Engagement scoring ──────────────────────────────────────────────────────

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


def _engagement_score(
    post: dict,
    total_score: int,
    total_comments: int,
    weights: dict[str, float],
) -> int:
    score_pct = percent(to_int(post.get("score", 0)), total_score)
    comments_pct = percent(to_int(post.get("num_comments", 0)), total_comments)
    upvote_pct = max(0.0, min(to_float(post.get("upvote_ratio", 0.0)) * 100.0, 100.0))

    engagement = (
        (weights.get("score", 0.0) * score_pct)
        + (weights.get("upvote_ratio", 0.0) * upvote_pct)
        + (weights.get("comments", 0.0) * comments_pct)
    )
    return max(0, int(round(engagement)))


# ─── Per-symbol fetch ────────────────────────────────────────────────────────

def _fetch_events_for_symbol(
    symbol: str,
    section: dict,
    weights: dict[str, float],
    lookback_minutes: int | None = None,
) -> list[dict]:
    """Fetch Reddit posts for a single *symbol* from its dedicated subreddits."""
    subreddits = _subreddits_for_symbol(section, symbol)
    keywords = _keywords_for_symbol(section, symbol)

    if not subreddits or not keywords:
        logger.warning("No subreddits/keywords configured for symbol=%s — skipping.", symbol)
        return []

    events: list[dict] = []
    query = _combined_reddit_query(keywords)
    if not query:
        logger.warning("No usable reddit keywords for symbol=%s — skipping.", symbol)
        return []

    request_timeout = section.get("request_timeout_seconds", DEFAULT_REQUEST_TIMEOUT[1])
    try:
        timeout_seconds = max(1.0, float(request_timeout))
    except (TypeError, ValueError):
        timeout_seconds = DEFAULT_REQUEST_TIMEOUT[1]

    rate_limit_sleep_seconds = section.get("rate_limit_sleep_seconds", 0.1)
    try:
        sleep_seconds = max(0.0, float(rate_limit_sleep_seconds))
    except (TypeError, ValueError):
        sleep_seconds = 0.1

    if lookback_minutes is not None and int(lookback_minutes) <= 60:
        timeframe = "hour"
    elif lookback_minutes is not None and int(lookback_minutes) <= 24 * 60:
        timeframe = "day"
    else:
        timeframe = "week"

    session = requests.Session()
    session.headers.update(HEADERS)

    for sub in subreddits:
        url = f"https://www.reddit.com/r/{sub}/search.json"
        params = {
            "q": query,
            "restrict_sr": "on",
            "sort": "new",
            "t": timeframe,
            "limit": 100,
        }
        try:
            response = session.get(url, params=params, timeout=(DEFAULT_REQUEST_TIMEOUT[0], timeout_seconds))
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
                    "symbol": symbol,
                }
                try:
                    events.append(normalize_event(payload))
                except ValueError:
                    continue

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        except Exception as exc:
            logger.debug("Reddit fetch failed [symbol=%s sub=%s]: %s", symbol, sub, exc)
            continue

    session.close()

    return events


# ─── Public entry-point ──────────────────────────────────────────────────────

def fetch_reddit_events(
    lookback_minutes: int | None = None,
    emit_current_timestamp: bool = True,
) -> list[dict]:
    """Fetch Reddit posts for ALL configured symbols and return a combined event list.

    Each event carries a ``symbol`` field (e.g. ``'BTC'``, ``'ETH'``) so that
    downstream bronze → silver → gold stages can group sentiment per coin.
    """
    section = get_sources_section("reddit")
    lookback_hours = section.get("lookback_hours", 6)
    weights = _reddit_engagement_weights(section)
    symbols = _configured_symbols()

    all_events: list[dict] = []
    for symbol in symbols:
        logger.info("Fetching Reddit events for symbol=%s", symbol)
        symbol_events = _fetch_events_for_symbol(
            symbol,
            section,
            weights,
            lookback_minutes=lookback_minutes,
        )
        logger.info("  -> fetched %d raw events for %s", len(symbol_events), symbol)
        all_events.extend(symbol_events)

    return apply_ingestion_policies(
        all_events,
        lookback_hours=lookback_hours,
        lookback_minutes=lookback_minutes,
        emit_current_timestamp=emit_current_timestamp,
    )
