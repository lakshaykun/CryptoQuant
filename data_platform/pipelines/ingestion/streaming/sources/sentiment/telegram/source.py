import asyncio
import logging
import os
from datetime import datetime, timezone

from pathlib import Path
from typing import Any

from telethon import TelegramClient
from telethon.sessions import StringSession

from pipelines.ingestion.streaming.sources.sentiment.shared.ingestion_state import (
    apply_ingestion_policies,
)
from pipelines.ingestion.streaming.sources.sentiment.shared.normalization import (
    normalize_event,
)
from pipelines.ingestion.streaming.sources.sentiment.telegram.config import (
    DEFAULT_CHANNELS_BY_SYMBOL,
    DEFAULT_KEYWORDS_BY_SYMBOL,
)
from utils.env import load_env_file
from utils.source_config import get_sources_section, load_sources_config

load_env_file()
logger = logging.getLogger(__name__)


def _configured_symbols() -> list[str]:
    cfg = load_sources_config()
    raw = cfg.get("symbols", ["BTC"])
    if isinstance(raw, list):
        return [str(s).strip().upper() for s in raw if str(s).strip()]
    return ["BTC"]


def _channels_for_symbol(section: dict[str, Any], symbol: str) -> list[str]:
    by_sym = section.get("channels_by_symbol", {})
    raw = by_sym.get(symbol, [])
    if isinstance(raw, list) and raw:
        return [str(value).strip().lstrip("@") for value in raw if str(value).strip()]
    return [
        str(value).strip().lstrip("@")
        for value in DEFAULT_CHANNELS_BY_SYMBOL.get(symbol, [])
        if str(value).strip()
    ]


def _keywords_for_symbol(section: dict[str, Any], symbol: str) -> list[str]:
    by_sym = section.get("keywords_by_symbol", {})
    raw = by_sym.get(symbol, [])
    if isinstance(raw, list) and raw:
        return [str(value).strip().lower() for value in raw if str(value).strip()]
    return [
        str(value).strip().lower()
        for value in DEFAULT_KEYWORDS_BY_SYMBOL.get(symbol, [symbol])
        if str(value).strip()
    ]


def _max_messages_per_channel(section: dict[str, Any]) -> int:
    try:
        return max(1, int(section.get("max_messages_per_channel", 200)))
    except (TypeError, ValueError):
        return 200


def telegram_interval_seconds() -> int:
    section = get_sources_section("telegram")
    try:
        return max(1, int(section.get("interval_seconds", 180)))
    except (TypeError, ValueError):
        return 180


def _build_client() -> TelegramClient | None:
    api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
    api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
    if not api_id_raw or not api_hash:
        logger.warning("TELEGRAM_API_ID / TELEGRAM_API_HASH missing; telegram ingestion will publish 0 events")
        return None

    try:
        api_id = int(api_id_raw)
    except ValueError:
        logger.warning("TELEGRAM_API_ID must be an integer; telegram ingestion will publish 0 events")
        return None

    session_string = os.getenv("TELEGRAM_SESSION_STRING", "").strip()
    if session_string:
        return TelegramClient(StringSession(session_string), api_id, api_hash)

    session_name = os.getenv("TELEGRAM_SESSION_NAME", "").strip()
    if not session_name:
        data_platform_root = Path(__file__).resolve().parents[6]
        session_path = data_platform_root / "logs" / "sentiment_pipeline" / "telethon_session"
        session_path.parent.mkdir(parents=True, exist_ok=True)
        session_name = str(session_path)

    return TelegramClient(session_name, api_id, api_hash)


def _message_replies(message: Any) -> int:
    replies = getattr(message, "replies", None)
    if replies is None:
        return 0
    return max(0, int(getattr(replies, "replies", 0) or 0))


def _message_reactions(message: Any) -> int:
    reactions = getattr(message, "reactions", None)
    if reactions is None:
        return 0

    total = 0
    for result in getattr(reactions, "results", []) or []:
        total += max(0, int(getattr(result, "count", 0) or 0))
    return total


def _message_matches_symbol(text: str, keywords: list[str]) -> bool:
    if not keywords:
        return True
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def _build_message_event(
    message: Any,
    channel_ref: str,
    symbol: str,
    keywords: list[str],
    channel_entity_id: int | None,
) -> dict[str, Any] | None:
    text = str(getattr(message, "message", "") or "").strip()
    if not text or not _message_matches_symbol(text, keywords):
        return None

    message_id = int(getattr(message, "id", 0) or 0)
    if message_id <= 0:
        return None

    sent_at = getattr(message, "date", None)
    if isinstance(sent_at, datetime):
        timestamp = sent_at.astimezone(timezone.utc).isoformat()
    else:
        timestamp = datetime.now(timezone.utc).isoformat()

    views = max(0, int(getattr(message, "views", 0) or 0))
    forwards = max(0, int(getattr(message, "forwards", 0) or 0))
    replies = _message_replies(message)
    reactions = _message_reactions(message)

    engagement = max(1, int(round((0.02 * views) + (2.0 * forwards) + (2.5 * replies) + reactions)))
    entity_id = channel_entity_id if channel_entity_id is not None else channel_ref

    payload = {
        "id": f"tg_{symbol}_{entity_id}_{message_id}",
        "timestamp": timestamp,
        "source": "telegram",
        "text": text,
        "engagement": engagement,
        "symbol": symbol,
    }

    try:
        return normalize_event(payload)
    except ValueError:
        return None


async def _fetch_events_for_symbol(
    client: TelegramClient,
    section: dict[str, Any],
    symbol: str,
    limit: int,
) -> list[dict[str, Any]]:
    channels = _channels_for_symbol(section, symbol)
    keywords = _keywords_for_symbol(section, symbol)
    if not channels:
        logger.warning("No Telegram channels configured for symbol=%s - skipping", symbol)
        return []

    symbol_events: list[dict[str, Any]] = []
    for channel_ref in channels:
        try:
            entity = await client.get_entity(channel_ref)
        except Exception as exc:
            logger.debug("Telegram get_entity failed [symbol=%s channel=%s]: %s", symbol, channel_ref, exc)
            continue

        channel_entity_id = getattr(entity, "id", None)

        try:
            async for message in client.iter_messages(entity, limit=limit):
                event = _build_message_event(message, channel_ref, symbol, keywords, channel_entity_id)
                if event is not None:
                    symbol_events.append(event)
        except Exception as exc:
            logger.debug("Telegram iter_messages failed [symbol=%s channel=%s]: %s", symbol, channel_ref, exc)
            continue

    return symbol_events


async def _fetch_telegram_events_async(
    lookback_minutes: int | None = None,
    emit_current_timestamp: bool = True,
) -> list[dict[str, Any]]:
    client = _build_client()
    if client is None:
        return []

    section = get_sources_section("telegram")
    lookback_hours = section.get("lookback_hours", 6)
    per_channel_limit = _max_messages_per_channel(section)
    symbols = _configured_symbols()
    all_events: list[dict[str, Any]] = []

    try:
        await client.connect()

        bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if bot_token:
            await client.start(bot_token=bot_token)
        elif not await client.is_user_authorized():
            logger.warning(
                "Telegram session is not authorized. Set TELEGRAM_BOT_TOKEN or TELEGRAM_SESSION_STRING to enable ingestion."
            )
            return []

        for symbol in symbols:
            logger.info("Fetching Telegram events for symbol=%s", symbol)
            symbol_events = await _fetch_events_for_symbol(client, section, symbol, per_channel_limit)
            logger.info("  -> fetched %d raw events for %s", len(symbol_events), symbol)
            all_events.extend(symbol_events)
    finally:
        await client.disconnect()

    return apply_ingestion_policies(
        all_events,
        lookback_hours=lookback_hours,
        lookback_minutes=lookback_minutes,
        emit_current_timestamp=emit_current_timestamp,
    )


def fetch_telegram_events(
    lookback_minutes: int | None = None,
    emit_current_timestamp: bool = True,
) -> list[dict[str, Any]]:
    try:
        return asyncio.run(
            _fetch_telegram_events_async(
                lookback_minutes=lookback_minutes,
                emit_current_timestamp=emit_current_timestamp,
            )
        )
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                _fetch_telegram_events_async(
                    lookback_minutes=lookback_minutes,
                    emit_current_timestamp=emit_current_timestamp,
                )
            )
        finally:
            loop.close()
