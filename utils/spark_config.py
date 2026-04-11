from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


DEFAULT_SPARK_CONFIG: dict[str, Any] = {
	"spark": {
		"app_name": "btc-sentiment-platform",
		"master": "local[*]",
	},
	"gold": {
		"window_seconds": 60,
		"watermark_seconds": 120,
		"cron_minutes": 5,
		"sentiment_endpoint": "http://127.0.0.1:8000/predict",
		"sentiment_timeout_seconds": 10,
	},
	"delta": {
		"bronze": "delta/bronze",
		"silver": "delta/silver",
		"gold": "delta/gold",
	},
	"checkpoints": {
		"bronze": "checkpoints/bronze",
		"silver": "checkpoints/silver",
		"gold": "checkpoints/gold",
	},
	"kafka": {
		"bootstrap_servers": "localhost:9092",
		"subscribe": "btc_reddit,btc_yt,btc_news",
		"starting_offsets": "latest",
		"fail_on_data_loss": False,
	},
}


@lru_cache(maxsize=1)
def load_spark_config(path: str | Path = "configs/spark.yaml") -> dict[str, Any]:
	config_path = Path(path)
	if not config_path.exists():
		return DEFAULT_SPARK_CONFIG

	loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
	if not isinstance(loaded, dict):
		return DEFAULT_SPARK_CONFIG

	merged = {
		"spark": dict(DEFAULT_SPARK_CONFIG["spark"]),
		"gold": dict(DEFAULT_SPARK_CONFIG["gold"]),
		"delta": dict(DEFAULT_SPARK_CONFIG["delta"]),
		"checkpoints": dict(DEFAULT_SPARK_CONFIG["checkpoints"]),
		"kafka": dict(DEFAULT_SPARK_CONFIG["kafka"]),
	}

	for section in ("spark", "gold", "delta", "checkpoints", "kafka"):
		candidate = loaded.get(section, {})
		if isinstance(candidate, dict):
			merged[section].update(candidate)

	return merged


def _section(name: str) -> dict[str, Any]:
	value = load_spark_config().get(name, {})
	if not isinstance(value, dict):
		return {}
	return value


def get_spark_app_name(default: str = "btc-sentiment-platform") -> str:
	app_name = str(_section("spark").get("app_name", default)).strip()
	return app_name or default


def get_spark_master(default: str = "local[*]") -> str:
	master = str(_section("spark").get("master", default)).strip()
	return master or default


def get_gold_window_seconds(default: int = 300) -> int:
	value = _section("gold").get("window_seconds", default)
	try:
		seconds = int(value)
		return seconds if seconds > 0 else default
	except (TypeError, ValueError):
		return default


def get_gold_watermark_seconds(default: int = 600) -> int:
	value = _section("gold").get("watermark_seconds", default)
	try:
		seconds = int(value)
		return seconds if seconds > 0 else default
	except (TypeError, ValueError):
		return default


def get_gold_sentiment_endpoint(default: str = "http://127.0.0.1:8000/predict") -> str:
	endpoint = str(_section("gold").get("sentiment_endpoint", default)).strip()
	return endpoint or default


def get_gold_sentiment_timeout_seconds(default: int = 10) -> int:
	value = _section("gold").get("sentiment_timeout_seconds", default)
	try:
		seconds = int(value)
		return seconds if seconds > 0 else default
	except (TypeError, ValueError):
		return default


def get_gold_cron_minutes(default: int = 5) -> int:
	value = _section("gold").get("cron_minutes", default)
	try:
		minutes = int(value)
		return minutes if minutes > 0 else default
	except (TypeError, ValueError):
		return default


def get_delta_path(layer: str, default: str) -> str:
	path = str(_section("delta").get(layer, default)).strip()
	return path or default


def get_checkpoint_path(layer: str, default: str) -> str:
	path = str(_section("checkpoints").get(layer, default)).strip()
	return path or default


def get_kafka_option(name: str, default: str) -> str:
	value = _section("kafka").get(name, default)
	if isinstance(value, bool):
		return "true" if value else "false"
	text = str(value).strip()
	return text or default
