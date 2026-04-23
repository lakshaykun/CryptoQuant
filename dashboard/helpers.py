from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import pandas as pd


def resolve_project_path(raw_path: str, repo_root: Path) -> Path:
    if raw_path.startswith("/opt/app/"):
        return repo_root / raw_path.replace("/opt/app/", "", 1)

    path = Path(raw_path)
    if path.is_absolute():
        return path

    return repo_root / path


def to_utc_datetime(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.to_datetime(series, errors="coerce", utc=True)

    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce")
        non_null = numeric.dropna()
        if non_null.empty:
            return pd.to_datetime(series, errors="coerce", utc=True)

        max_abs = float(non_null.abs().max())
        unit = "ms" if max_abs > 1e12 else "s"
        return pd.to_datetime(numeric, unit=unit, errors="coerce", utc=True)

    return pd.to_datetime(series, errors="coerce", utc=True)


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
