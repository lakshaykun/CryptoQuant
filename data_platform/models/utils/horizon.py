from __future__ import annotations


def parse_interval(interval_str: str) -> int:
    value = (interval_str or "").strip().lower()
    if value.endswith("m"):
        return int(value[:-1])
    if value.endswith("h"):
        return int(value[:-1]) * 60
    raise ValueError(f"Unsupported interval format: '{interval_str}'")


def compute_steps_per_day(interval_str: str) -> int:
    interval_minutes = parse_interval(interval_str)
    if interval_minutes <= 0:
        raise ValueError("Interval minutes must be > 0")
    steps = 1440 / interval_minutes
    if steps < 1:
        raise ValueError("Interval cannot be greater than one day")
    return int(steps)


def resolve_horizon_steps(horizon: str, interval_str: str) -> int:
    normalized = (horizon or "short").strip().lower()
    if normalized == "short":
        return 1
    if normalized in {"1d", "day", "long"}:
        return compute_steps_per_day(interval_str)
    raise ValueError(f"Unsupported horizon value: '{horizon}'")
