from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import streamlit as st

from delta_client import get_model_versions
from settings import TIME_RANGE_OPTIONS

_RANGE_MAP = {
    "6 hours":  timedelta(hours=6),
    "12 hours": timedelta(hours=12),
    "1 day":    timedelta(days=1),
    "7 days":   timedelta(days=7),
    "1 month":  timedelta(days=30),
    "1 year":   timedelta(days=365),
}


def build_time_window(selection: str) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    if selection in _RANGE_MAP:
        return now - _RANGE_MAP[selection], now
    # "Max" — no lower bound, use a 10-year lookback
    return now - timedelta(days=3650), now


def render_sidebar_filters(
    data_config: Dict[str, Any],
    refresh_nonce: int,
    repo_root: str,
) -> Dict[str, Any]:
    st.sidebar.header("⚙️ Controls")

    refresh_clicked = st.sidebar.button("🔄 Refresh", use_container_width=True)

    symbols = data_config.get("symbols", []) or ["BTCUSDT"]
    symbol = st.sidebar.selectbox("Symbol", options=symbols, key="sym")

    time_range = st.sidebar.selectbox(
        "Time window", options=TIME_RANGE_OPTIONS, index=2, key="trange"
    )
    start, end = build_time_window(time_range)

    predictions_path = (
        data_config.get("tables", {})
        .get("predictions_log_return_lead1", {})
        .get("path", "")
    )
    versions = get_model_versions(
        predictions_path=predictions_path,
        repo_root=repo_root,
        refresh_nonce=refresh_nonce,
    )
    model_version = st.sidebar.selectbox(
        "Model version",
        options=["All"] + [v for v in versions if v != "All"],
        key="mver",
    )

    return {
        "refresh_clicked": refresh_clicked,
        "symbol": symbol,
        "start": start,
        "end": end,
        "model_version": model_version,
    }
