from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from helpers import resolve_project_path, to_utc_datetime

try:
    from deltalake import DeltaTable
except Exception:
    DeltaTable = None


def _available_columns(table: DeltaTable) -> List[str]:
    try:
        return list(table.schema().to_pyarrow().names)
    except Exception:
        return []


@st.cache_data(show_spinner=False, ttl=20)
def load_delta_table(
    table_path: str,
    repo_root: str,
    columns: Optional[List[str]] = None,
    symbol: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    refresh_nonce: int = 0,
) -> pd.DataFrame:
    _ = refresh_nonce

    if DeltaTable is None or not table_path:
        return pd.DataFrame()

    resolved_path = resolve_project_path(table_path, Path(repo_root))
    if not resolved_path.exists():
        return pd.DataFrame()

    try:
        table = DeltaTable(str(resolved_path))
    except Exception:
        return pd.DataFrame()

    available_cols = _available_columns(table)
    if not available_cols:
        return pd.DataFrame()

    selected_columns = columns or available_cols
    selected_columns = [col for col in selected_columns if col in available_cols]
    if not selected_columns:
        return pd.DataFrame()

    filters: List[Tuple[str, str, Any]] = []
    if symbol and "symbol" in available_cols:
        filters.append(("symbol", "=", symbol))

    if start and end and "date" in available_cols:
        filters.append(("date", ">=", start.date()))
        filters.append(("date", "<=", end.date()))

    try:
        if filters:
            frame = table.to_pandas(columns=selected_columns, filters=filters)
        else:
            frame = table.to_pandas(columns=selected_columns)
    except Exception:
        try:
            frame = table.to_pandas(columns=selected_columns)
        except Exception:
            return pd.DataFrame()

    if frame.empty:
        return frame

    for time_col in ["open_time", "ingestion_time", "event_time"]:
        if time_col in frame.columns:
            frame[time_col] = to_utc_datetime(frame[time_col])

    if start and end:
        for preferred_time_col in ["open_time", "event_time", "ingestion_time"]:
            if preferred_time_col in frame.columns:
                frame = frame[
                    (frame[preferred_time_col] >= start)
                    & (frame[preferred_time_col] <= end)
                ]
                break

    sort_cols = [col for col in ["open_time", "event_time", "ingestion_time"] if col in frame.columns]
    if sort_cols:
        return frame.sort_values(by=sort_cols)

    return frame


@st.cache_data(show_spinner=False, ttl=60)
def get_model_versions(predictions_path: str, repo_root: str, refresh_nonce: int = 0) -> List[str]:
    _ = refresh_nonce

    if DeltaTable is None or not predictions_path:
        return ["default"]

    resolved_path = resolve_project_path(predictions_path, Path(repo_root))
    if not resolved_path.exists():
        return ["default"]

    try:
        table = DeltaTable(str(resolved_path))
        cols = _available_columns(table)
        if "model_version" not in cols:
            return ["default"]

        frame = table.to_pandas(columns=["model_version"])  # noqa: PD901
        versions = sorted({str(v) for v in frame["model_version"].dropna().unique().tolist()})
        return versions if versions else ["default"]
    except Exception:
        return ["default"]
