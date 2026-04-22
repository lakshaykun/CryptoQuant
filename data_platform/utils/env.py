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


def _candidate_env_paths(file_path: str | Path | None = None) -> list[Path]:
	if file_path is not None:
		return [Path(file_path)]

	this_file = Path(__file__).resolve()
	data_platform_root = this_file.parents[1]
	workspace_root = this_file.parents[2]
	cwd = Path.cwd()

	# Search most likely locations first while keeping legacy support.
	return [
		workspace_root / ".env",
		data_platform_root / ".env",
		cwd / ".env",
		data_platform_root / "configs" / "app.env",
	]


def load_env_file(file_path: str | Path | None = None) -> dict[str, str]:
	# Always try both workspace and data_platform .env files, merging all found.
	paths = _candidate_env_paths(file_path)
	# Add both workspace and data_platform .env explicitly, in case cwd is different.
	this_file = Path(__file__).resolve()
	data_platform_root = this_file.parents[1]
	workspace_root = this_file.parents[2]
	extra_paths = [workspace_root / ".env", data_platform_root / ".env"]
	seen = set()
	all_paths = []
	for p in extra_paths + paths:
		if p.exists() and str(p) not in seen:
			all_paths.append(p)
			seen.add(str(p))

	loaded: dict[str, str] = {}
	for path in all_paths:
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
