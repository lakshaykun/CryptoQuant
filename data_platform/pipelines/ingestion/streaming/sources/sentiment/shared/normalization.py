from dataclasses import asdict, dataclass

from utils.text_cleaner import clean_text
from utils.time_utils import normalize_timestamp


@dataclass
class SentimentEvent:
    id: str
    timestamp: str
    source: str
    text: str
    engagement: int = 1
    symbol: str = "BTC"


def normalize_event(payload: dict) -> dict:
    event = SentimentEvent(
        id=str(payload.get("id", "")).strip(),
        timestamp=normalize_timestamp(payload.get("timestamp")),
        source=str(payload.get("source", "unknown")).strip().lower(),
        text=clean_text(str(payload.get("text", ""))),
        # Engagement is intentionally flattened to 1 so source-specific
        # "total engagement" logic does not bias downstream sentiment.
        engagement=1,
        symbol=str(payload.get("symbol", "BTC")).strip().upper(),
    )

    if not event.id:
        raise ValueError("Event id is required")
    if not event.text:
        raise ValueError("Event text is required")

    return asdict(event)


def normalized_weights(raw_weights: dict, defaults: dict[str, float]) -> dict[str, float]:
    parsed: dict[str, float] = {}
    for key, default_value in defaults.items():
        try:
            parsed[key] = max(0.0, float(raw_weights.get(key, default_value)))
        except (TypeError, ValueError):
            parsed[key] = default_value

    total = sum(parsed.values())
    if total <= 0:
        return defaults.copy()

    return {key: value / total for key, value in parsed.items()}
