from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

from utils.time_utils import utc_now_iso

# Keep lightweight process-local state for repeated polling runs.
_last_engagement_by_event_id: dict[str, int] = {}


def _parse_utc_timestamp(timestamp_text: str | None) -> datetime | None:
    if not timestamp_text:
        return None

    text = str(timestamp_text).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(text).astimezone(UTC)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except (TypeError, ValueError):
            return None


def _normalize_lookback_hours(raw_value: Any, default_hours: int = 72) -> int:
    try:
        value = int(raw_value)
        return value if value > 0 else default_hours
    except (TypeError, ValueError):
        return default_hours


def apply_ingestion_policies(
    events: list[dict[str, Any]],
    lookback_hours: int | str | None = 72,
) -> list[dict[str, Any]]:
    if not events:
        return []

    now_utc = datetime.now(UTC)
    threshold = now_utc - timedelta(hours=_normalize_lookback_hours(lookback_hours))
    accepted: list[dict[str, Any]] = []

    for event in events:
        event_id = str(event.get("id", "")).strip()
        if not event_id:
            continue

        event_time = _parse_utc_timestamp(event.get("timestamp"))
        if event_time is None or event_time < threshold:
            continue

        current_engagement = max(0, int(event.get("engagement", 0) or 0))
        previous_engagement = _last_engagement_by_event_id.get(event_id)

        if previous_engagement is None:
            _last_engagement_by_event_id[event_id] = current_engagement
            first_seen = dict(event)
            first_seen["timestamp"] = utc_now_iso()
            accepted.append(first_seen)
            continue

        engagement_delta = max(0, current_engagement - previous_engagement)
        _last_engagement_by_event_id[event_id] = max(previous_engagement, current_engagement)

        updated = dict(event)
        updated["engagement"] = engagement_delta
        updated["timestamp"] = utc_now_iso()
        accepted.append(updated)

    return accepted
