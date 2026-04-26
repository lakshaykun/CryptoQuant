from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from utils.config_loader import load_config


SUPPORTED_SENTIMENT_SOURCES = ("telegram", "youtube", "reddit", "news")


@dataclass(frozen=True)
class SentimentExecutionConfig:
    mode: str
    sources: list[str]
    bronze_table: str
    silver_table: str
    gold_table: str
    bronze_state_layer: str
    silver_state_layer: str
    gold_state_layer: str
    start_time: datetime | None
    lookback_minutes: int | None
    fallback_lookback_minutes: int
    min_lookback_minutes: int
    query_safety_buffer_minutes: int
    source_aliases: dict[str, list[str]]


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


def _normalize_sources(raw_sources: Any) -> list[str]:
    if not isinstance(raw_sources, list):
        return list(SUPPORTED_SENTIMENT_SOURCES)

    normalized: list[str] = []
    for source in raw_sources:
        candidate = str(source).strip().lower()
        if candidate in SUPPORTED_SENTIMENT_SOURCES and candidate not in normalized:
            normalized.append(candidate)

    return normalized or list(SUPPORTED_SENTIMENT_SOURCES)


def _normalize_source_aliases(raw_aliases: Any) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    if not isinstance(raw_aliases, dict):
        raw_aliases = {}

    for source in SUPPORTED_SENTIMENT_SOURCES:
        aliases_raw = raw_aliases.get(source, [source])
        aliases: list[str] = []
        if isinstance(aliases_raw, list):
            for alias in aliases_raw:
                candidate = str(alias).strip().lower()
                if candidate and candidate not in aliases:
                    aliases.append(candidate)
        if source not in aliases:
            aliases.append(source)
        normalized[source] = aliases

    return normalized


def load_sentiment_pipeline_config(mode: str, path: str = "configs/sentiment_pipeline.yaml") -> SentimentExecutionConfig:
    config = load_config(path) or {}
    if not isinstance(config, dict):
        config = {}

    mode_key = "streaming" if mode == "streaming" else "batch"
    mode_cfg = config.get(mode_key, {})
    if not isinstance(mode_cfg, dict):
        mode_cfg = {}

    common_min_lookback = _parse_positive_int(config.get("min_lookback_minutes"), 1)
    common_buffer = _parse_positive_int(config.get("query_safety_buffer_minutes"), 2)

    return SentimentExecutionConfig(
        mode=mode_key,
        sources=_normalize_sources(mode_cfg.get("sources")),
        bronze_table=str(mode_cfg.get("bronze_table", "bronze_sentiment")).strip(),
        silver_table=str(mode_cfg.get("silver_table", "silver_sentiment")).strip(),
        gold_table=str(mode_cfg.get("gold_table", "gold_sentiment")).strip(),
        bronze_state_layer=str(mode_cfg.get("bronze_state_layer", f"{mode_key}_bronze")).strip(),
        silver_state_layer=str(mode_cfg.get("silver_state_layer", f"{mode_key}_silver")).strip(),
        gold_state_layer=str(mode_cfg.get("gold_state_layer", f"{mode_key}_gold")).strip(),
        start_time=_parse_datetime(mode_cfg.get("start_date")) if mode_key == "batch" else None,
        lookback_minutes=_parse_positive_int(mode_cfg.get("lookback_minutes"), 30) if mode_key == "streaming" else None,
        fallback_lookback_minutes=_parse_positive_int(mode_cfg.get("fallback_lookback_minutes"), 1440),
        min_lookback_minutes=common_min_lookback,
        query_safety_buffer_minutes=common_buffer,
        source_aliases=_normalize_source_aliases(config.get("source_aliases")),
    )
