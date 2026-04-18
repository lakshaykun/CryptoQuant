import os
from pathlib import Path


def _parse_env_line(line: str) -> tuple[str, str] | None:
	line = line.strip()
	if not line or line.startswith("#"):
		return None

	# Accept both KEY=value and KEY = value formats.
	if "=" not in line:
		return None

	key, value = line.split("=", 1)
	key = key.strip()
	value = value.strip().strip('"').strip("'")
	if not key:
		return None
	return key, value


def load_env_file(file_path: str | Path = "configs/app.env") -> dict[str, str]:
	path = Path(file_path)
	loaded: dict[str, str] = {}

	if not path.exists():
		return loaded

	for line in path.read_text(encoding="utf-8").splitlines():
		parsed = _parse_env_line(line)
		if not parsed:
			continue

		key, value = parsed
		os.environ.setdefault(key, value)
		loaded[key] = value

	return loaded


def get_env(name: str, default: str | None = None, required: bool = False) -> str:
	value = os.getenv(name, default)
	if required and (value is None or value == ""):
		raise ValueError(f"Missing required environment variable: {name}")
	return "" if value is None else value


# Load default environment file on import for convenience in scripts.
load_env_file()
