from datetime import datetime, timezone

def utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


def normalize_timestamp(value: str | int | float | None) -> str:
	if value is None:
		return utc_now_iso()

	if isinstance(value, (int, float)):
		return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()

	# Keep already-ISO timestamps untouched, normalize common "Z" suffix.
	text = str(value).strip()
	if text.endswith("Z"):
		text = text[:-1] + "+00:00"

	try:
		return datetime.fromisoformat(text).astimezone(timezone.utc).isoformat()
	except ValueError:
		# Fall back to epoch string handling.
		try:
			return datetime.fromtimestamp(float(text), tz=timezone.utc).isoformat()
		except ValueError:
			return utc_now_iso()
