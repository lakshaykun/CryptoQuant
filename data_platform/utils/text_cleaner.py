import re


URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
MENTION_RE = re.compile(r"[@#]\w+")
MULTISPACE_RE = re.compile(r"\s+")


def clean_text(text: str) -> str:
	if not text:
		return ""

	cleaned = URL_RE.sub(" ", text)
	cleaned = MENTION_RE.sub(" ", cleaned)
	cleaned = cleaned.replace("\n", " ")
	cleaned = MULTISPACE_RE.sub(" ", cleaned).strip()
	return cleaned.lower()
