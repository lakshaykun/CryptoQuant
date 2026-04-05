from dataclasses import asdict, dataclass

from utils.text_cleaner import clean_text
from utils.time_utils import normalize_timestamp


@dataclass
class SentimentEvent:
	id: str
	timestamp: str
	source: str
	text: str
	engagement: int
	symbol: str = "BTC"


def normalize_event(payload: dict) -> dict:
	event = SentimentEvent(
		id=str(payload.get("id", "")).strip(),
		timestamp=normalize_timestamp(payload.get("timestamp")),
		source=str(payload.get("source", "unknown")).strip().lower(),
		text=clean_text(str(payload.get("text", ""))),
		engagement=max(int(payload.get("engagement", 0)), 0),
		symbol=str(payload.get("symbol", "BTC")).strip().upper(),
	)

	if not event.id:
		raise ValueError("Event id is required")
	if not event.text:
		raise ValueError("Event text is required")

	return asdict(event)
