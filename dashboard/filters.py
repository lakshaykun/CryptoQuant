from __future__ import annotations

from datetime import datetime, timedelta, timezone, time as dt_time
from typing import Any, Dict

import streamlit as st

from delta_client import get_model_versions
from settings import DEFAULT_PROMETHEUS_URL, TIME_RANGE_OPTIONS


def build_time_window(selection: str) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)

    mapping = {
        "Last 15 min": timedelta(minutes=15),
        "1 hour": timedelta(hours=1),
        "6 hours": timedelta(hours=6),
        "24 hours": timedelta(hours=24),
    }

    if selection in mapping:
        return now - mapping[selection], now

    default_start = now - timedelta(hours=1)

    start_date = st.sidebar.date_input("Start date", value=default_start.date(), key="custom_start_date")
    start_time = st.sidebar.time_input(
        "Start time",
        value=dt_time(default_start.hour, default_start.minute),
        key="custom_start_time",
    )
    end_date = st.sidebar.date_input("End date", value=now.date(), key="custom_end_date")
    end_time = st.sidebar.time_input("End time", value=dt_time(now.hour, now.minute), key="custom_end_time")

    start = datetime.combine(start_date, start_time).replace(tzinfo=timezone.utc)
    end = datetime.combine(end_date, end_time).replace(tzinfo=timezone.utc)

    if end <= start:
        st.sidebar.warning("End time must be after start time. Using a 1-hour window.")
        return now - timedelta(hours=1), now

    return start, end


def render_sidebar_filters(data_config: Dict[str, Any], refresh_nonce: int, repo_root: str) -> Dict[str, Any]:
    st.sidebar.header("Filters")

    refresh_clicked = st.sidebar.button("Refresh now", width="stretch")
    st.sidebar.caption("Auto-refresh: every 30 seconds")

    symbols = data_config.get("symbols", []) or ["BTCUSDT"]
    symbol = st.sidebar.selectbox("Symbol", options=symbols, index=0, key="symbol_filter")

    time_range = st.sidebar.selectbox(
        "Time range",
        options=TIME_RANGE_OPTIONS,
        index=2,
        key="time_range_filter",
    )
    start, end = build_time_window(time_range)

    predictions_path = data_config.get("tables", {}).get("predictions_log_return_lead1", {}).get("path", "")
    model_versions = get_model_versions(
        predictions_path=predictions_path,
        repo_root=repo_root,
        refresh_nonce=refresh_nonce,
    )
    model_versions = ["All"] + [version for version in model_versions if version != "All"]

    model_version = st.sidebar.selectbox(
        "Model version",
        options=model_versions,
        index=0,
        key="model_version_filter",
    )

    prometheus_url = st.sidebar.text_input(
        "Prometheus URL",
        value=DEFAULT_PROMETHEUS_URL,
        key="prometheus_url_filter",
    )

    return {
        "refresh_clicked": refresh_clicked,
        "symbol": symbol,
        "start": start,
        "end": end,
        "model_version": model_version,
        "prometheus_url": prometheus_url,
    }
