from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

from utils.time_utils import utc_now_iso

# Keep lightweight process-local state for repeated polling runs.
_last_engagement_by_event_id: dict[str, int] = {}
_last_emitted_by_event_id: dict[str, datetime] = {}


def parse_utc_timestamp(timestamp_text: str | None) -> datetime | None:
    if not timestamp_text:
        return None

    text = str(timestamp_text).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except (TypeError, ValueError):
            return None


def _normalize_lookback_hours(raw_value: Any, default_hours: int = 72) -> int:
    try:
        value = int(raw_value)
        return value if value > 0 else default_hours
    except (TypeError, ValueError):
        return default_hours


def _normalize_lookback_minutes(raw_value: Any, default_minutes: int = 360) -> int:
    try:
        value = int(raw_value)
        return value if value > 0 else default_minutes
    except (TypeError, ValueError):
        return default_minutes


def apply_ingestion_policies(
    events: list[dict[str, Any]],
    lookback_hours: int | str | None = 72,
    lookback_minutes: int | str | None = None,
    emit_current_timestamp: bool = True,
) -> list[dict[str, Any]]:
    if not events:
        return []

    now_utc = datetime.now(timezone.utc)
    if lookback_minutes is not None:
        threshold = now_utc - timedelta(minutes=_normalize_lookback_minutes(lookback_minutes))
    else:
        threshold = now_utc - timedelta(hours=_normalize_lookback_hours(lookback_hours))
    accepted: list[dict[str, Any]] = []
    seen_in_batch: set[str] = set()

    for event in events:
        event_id = str(event.get("id", "")).strip()
        if not event_id:
            continue
        if event_id in seen_in_batch:
            continue
        seen_in_batch.add(event_id)

        event_time = parse_utc_timestamp(event.get("timestamp"))
        if event_time is None or event_time < threshold:
            continue

        # Skip already-emitted ids to avoid duplicate payloads in streaming loops.
        last_emitted = _last_emitted_by_event_id.get(event_id)
        if last_emitted is not None and last_emitted >= threshold:
            continue

        current_engagement = max(0, int(event.get("engagement", 0) or 0))
        previous_engagement = _last_engagement_by_event_id.get(event_id)

        if previous_engagement is None:
            _last_engagement_by_event_id[event_id] = current_engagement
            first_seen = dict(event)
            if emit_current_timestamp:
                first_seen["timestamp"] = utc_now_iso()
            _last_emitted_by_event_id[event_id] = now_utc
            accepted.append(first_seen)
            continue

        engagement_delta = max(0, current_engagement - previous_engagement)
        _last_engagement_by_event_id[event_id] = max(previous_engagement, current_engagement)

        if engagement_delta <= 0:
            continue

        updated = dict(event)
        updated["engagement"] = engagement_delta
        if emit_current_timestamp:
            updated["timestamp"] = utc_now_iso()
        _last_emitted_by_event_id[event_id] = now_utc
        accepted.append(updated)

    return accepted
