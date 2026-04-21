import logging
import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from pipelines.ingestion.streaming.sources.sentiment.shared.ingestion_state import apply_ingestion_policies
from pipelines.ingestion.streaming.sources.sentiment.shared.normalization import normalize_event
from utils.env import load_env_file
from utils.number_utils import to_int
from utils.source_config import get_sources_section, load_sources_config

load_env_file()
logger = logging.getLogger(__name__)


# ─── Symbols ──────────────────────────────────────────────────────────────────

def _configured_symbols() -> list[str]:
    """Return the list of crypto symbols to track (e.g. BTC, ETH, SOL, BNB, XRP)."""
    cfg = load_sources_config()
    raw = cfg.get("symbols", ["BTC"])
    if isinstance(raw, list):
        return [str(s).strip().upper() for s in raw if str(s).strip()]
    return ["BTC"]


# ─── Per-symbol config helpers ────────────────────────────────────────────────

def _channels_for_symbol(section: dict, symbol: str) -> list[str]:
    """Return YouTube channel IDs dedicated to *symbol*."""
    by_sym = section.get("channels_by_symbol", {})
    raw = by_sym.get(symbol, [])
    if isinstance(raw, list):
        return [str(c).strip() for c in raw if str(c).strip().startswith("UC")]
    # Legacy fallback: flat channel_ids list
    ids = section.get("channel_ids", [])
    return [str(c).strip() for c in ids if str(c).strip().startswith("UC")]


def _search_term_for_symbol(section: dict, symbol: str) -> str:
    """Return the YouTube comment search term for *symbol*."""
    by_sym = section.get("search_terms_by_symbol", {})
    term = by_sym.get(symbol)
    if term:
        return str(term).strip()
    # Sensible defaults when key is absent
    defaults = {
        "BTC": "bitcoin BTC",
        "ETH": "ethereum ETH",
        "SOL": "solana SOL",
        "BNB": "binance BNB",
        "XRP": "ripple XRP",
    }
    return defaults.get(symbol, symbol.lower())


def _configured_channel_limit(section: dict) -> int:
    raw_limit = section.get("max_comments_per_channel")
    try:
        return max(1, int(raw_limit))
    except (TypeError, ValueError):
        return 200


# ─── YouTube API helpers ──────────────────────────────────────────────────────

def _youtube_client():
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    if not youtube_api_key:
        logger.warning("YOUTUBE_API_KEY is missing; youtube ingestion will publish 0 events")
        return None
    return build("youtube", "v3", developerKey=youtube_api_key)


def _video_like_percentage(youtube, video_id: str) -> float:
    response = youtube.videos().list(part="statistics", id=video_id).execute()
    items = response.get("items", [])
    if not items:
        return 0.0

    stats = items[0].get("statistics", {})
    like_count = to_int(stats.get("likeCount", 0))
    view_count = to_int(stats.get("viewCount", 0))
    if view_count <= 0:
        return 0.0
    return (like_count / view_count) * 100.0


def _list_comment_threads_for_channel(
    youtube,
    channel_id: str,
    search_term: str,
    max_results: int,
) -> list[dict]:
    """Fetch up to *max_results* comment threads from *channel_id* filtered by *search_term*."""
    items: list[dict] = []
    next_page_token: str | None = None

    while len(items) < max_results:
        response = youtube.commentThreads().list(
            part="snippet",
            allThreadsRelatedToChannelId=channel_id,
            searchTerms=search_term,
            maxResults=min(max_results - len(items), 100),
            order="time",
            pageToken=next_page_token,
        ).execute()

        page_items = response.get("items", [])
        if not page_items:
            break

        items.extend(page_items)
        if len(items) >= max_results:
            break

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return items[:max_results]


def _fetch_channel_comments(
    youtube,
    channel_id: str,
    search_term: str,
    symbol: str,
    max_results: int,
) -> list[dict]:
    """Fetch and normalize comments from *channel_id* for *symbol*."""
    if youtube is None or max_results <= 0:
        return []

    thread_items = _list_comment_threads_for_channel(youtube, channel_id, search_term, max_results)
    if not thread_items:
        return []

    comments: list[dict] = []
    comment_rows = []
    video_like_cache: dict[str, float] = {}

    for item in thread_items:
        snippet = item.get("snippet", {})
        top_level = snippet.get("topLevelComment", {}).get("snippet", {})
        video_id = top_level.get("videoId")
        comment_like_count = to_int(top_level.get("likeCount", 0))
        comment_rows.append((item, top_level, comment_like_count, video_id))

        if video_id and video_id not in video_like_cache:
            try:
                video_like_cache[video_id] = _video_like_percentage(youtube, video_id)
            except Exception:
                video_like_cache[video_id] = 0.0

    total_comment_likes = sum(row[2] for row in comment_rows)

    for item, comment, comment_like_count, video_id in comment_rows:
        comment_like_percent = 0.0
        if total_comment_likes > 0:
            comment_like_percent = (comment_like_count / total_comment_likes) * 100.0

        video_like_percent = 0.0
        if video_id:
            video_like_percent = video_like_cache.get(video_id, 0.0)

        payload = {
            "id": item.get("id", ""),
            "timestamp": comment.get("publishedAt"),
            "source": "youtube",
            "text": comment.get("textDisplay", ""),
            "engagement": int(round(video_like_percent + comment_like_percent)),
            # ← tagged with the correct coin symbol
            "symbol": symbol,
        }

        try:
            comments.append(normalize_event(payload))
        except ValueError:
            continue

    return comments


# ─── Per-symbol fetch ─────────────────────────────────────────────────────────

def _fetch_events_for_symbol(
    youtube,
    symbol: str,
    section: dict,
    limit: int,
) -> list[dict]:
    """Fetch YouTube comments for a single *symbol* from its dedicated channels."""
    channel_ids = _channels_for_symbol(section, symbol)
    search_term = _search_term_for_symbol(section, symbol)

    if not channel_ids:
        logger.warning("No YouTube channels configured for symbol=%s — skipping.", symbol)
        return []

    events: list[dict] = []
    for channel_id in channel_ids:
        try:
            channel_events = _fetch_channel_comments(youtube, channel_id, search_term, symbol, limit)
            events.extend(channel_events)
            logger.debug(
                "YouTube channel=%s symbol=%s fetched %d comments",
                channel_id, symbol, len(channel_events),
            )
        except HttpError as exc:
            logger.debug("YouTube HttpError channel=%s symbol=%s: %s", channel_id, symbol, exc)
            continue
        except Exception as exc:
            logger.debug("YouTube error channel=%s symbol=%s: %s", channel_id, symbol, exc)
            continue

    return events


# ─── Public entry-point ───────────────────────────────────────────────────────

def fetch_youtube_events(max_results_per_channel: int | None = None) -> list[dict]:
    """Fetch YouTube comments for ALL configured symbols and return a combined event list.

    Each event carries a ``symbol`` field (e.g. ``'BTC'``, ``'ETH'``) so that
    downstream bronze → silver → gold stages can group sentiment per coin.
    """
    youtube = _youtube_client()
    if youtube is None:
        return []

    section = get_sources_section("youtube")
    lookback_hours = section.get("lookback_hours", 6)
    limit = max_results_per_channel or _configured_channel_limit(section)

    symbols = _configured_symbols()
    all_events: list[dict] = []

    for symbol in symbols:
        logger.info("Fetching YouTube events for symbol=%s", symbol)
        symbol_events = _fetch_events_for_symbol(youtube, symbol, section, limit)
        logger.info("  -> fetched %d raw events for %s", len(symbol_events), symbol)
        all_events.extend(symbol_events)

    return apply_ingestion_policies(all_events, lookback_hours=lookback_hours)
