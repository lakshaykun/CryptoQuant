from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


@lru_cache(maxsize=1)
def load_sources_config(path: str | Path = "configs/sources.yaml") -> dict[str, Any]:
	config_path = Path(path)
	if not config_path.exists():
		return {}

	loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
	if not isinstance(loaded, dict):
		return {}
	return loaded


def get_sources_section(name: str) -> dict[str, Any]:
	section = load_sources_config().get(name, {})
	if not isinstance(section, dict):
		return {}
	return section


def get_list_value(section: dict[str, Any], key: str, default: list[str]) -> list[str]:
	raw = section.get(key, default)

	if isinstance(raw, list):
		values = [str(item).strip() for item in raw if str(item).strip()]
		return values or default

	if isinstance(raw, str):
		values = [item.strip() for item in raw.split(",") if item.strip()]
		return values or default

	return default
