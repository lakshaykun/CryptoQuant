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
    cfg = load_sources_config()
    raw = cfg.get("symbols", ["BTC"])
    if isinstance(raw, list):
        return [str(s).strip().upper() for s in raw if str(s).strip()]
    return ["BTC"]


# ─── Per-symbol config helpers ────────────────────────────────────────────────

def _channels_for_symbol(section: dict, symbol: str) -> list[str]:
    by_sym = section.get("channels_by_symbol", {})
    raw = by_sym.get(symbol, [])
    if isinstance(raw, list):
        return [str(c).strip() for c in raw if str(c).strip().startswith("UC")]

    ids = section.get("channel_ids", [])
    return [str(c).strip() for c in ids if str(c).strip().startswith("UC")]


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

    youtube = build("youtube", "v3", developerKey=youtube_api_key)

    try:
        response = youtube.videos().list(
            part="id",
            chart="mostPopular",
            maxResults=1
        ).execute()

        if "items" in response:
            logger.info("✅ YouTube API key is working correctly")
        else:
            logger.warning("⚠️ YouTube API responded but no data returned")

    except Exception as e:
        logger.error(f"❌ YouTube API key is INVALID or request failed: {e}")
        return None

    return youtube


def _list_comment_threads_for_channel(
    youtube,
    channel_id: str,
    max_results: int,
) -> list[dict]:

    items: list[dict] = []
    next_page_token: str | None = None

    while len(items) < max_results:
        response = youtube.commentThreads().list(
            part="snippet",
            allThreadsRelatedToChannelId=channel_id,
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
    symbol: str,
    max_results: int,
) -> list[dict]:

    if youtube is None or max_results <= 0:
        return []

    thread_items = _list_comment_threads_for_channel(
        youtube, channel_id, max_results
    )

    if not thread_items:
        return []

    comments: list[dict] = []

    for item in thread_items:
        snippet = item.get("snippet", {})
        top_level = snippet.get("topLevelComment", {}).get("snippet", {})

        comment_like_count = to_int(top_level.get("likeCount", 0))
        reply_count = to_int(snippet.get("totalReplyCount", 0))

        engagement = comment_like_count + reply_count

        if engagement == 0:
            engagement = 1

        payload = {
            "id": item.get("id", ""),
            "timestamp": top_level.get("publishedAt"),
            "source": "youtube",
            "text": top_level.get("textOriginal", ""),
            "engagement": engagement,
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

    channel_ids = _channels_for_symbol(section, symbol)

    if not channel_ids:
        logger.warning("No YouTube channels configured for symbol=%s — skipping.", symbol)
        return []

    events: list[dict] = []

    for channel_id in channel_ids:
        try:
            channel_events = _fetch_channel_comments(
                youtube, channel_id, symbol, limit
            )

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

def fetch_youtube_events(
    max_results_per_channel: int | None = None,
    lookback_minutes: int | None = None,
    emit_current_timestamp: bool = True,
) -> list[dict]:

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

        symbol_events = _fetch_events_for_symbol(
            youtube, symbol, section, limit
        )

        logger.info("  -> fetched %d raw events for %s", len(symbol_events), symbol)

        all_events.extend(symbol_events)

    return apply_ingestion_policies(
        all_events,
        lookback_hours=lookback_hours,
        lookback_minutes=lookback_minutes,
        emit_current_timestamp=emit_current_timestamp,
    )
