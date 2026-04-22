from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st

from helpers import safe_float


@st.cache_data(show_spinner=False, ttl=15)
def query_prometheus(
    base_url: str,
    query: str,
    query_time: Optional[datetime] = None,
    refresh_nonce: int = 0,
) -> Optional[float]:
    _ = refresh_nonce

    endpoint = f"{base_url.rstrip('/')}/api/v1/query"
    params: Dict[str, Any] = {"query": query}

    if query_time is not None:
        params["time"] = query_time.timestamp()

    try:
        response = requests.get(endpoint, params=params, timeout=4)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None

    if payload.get("status") != "success":
        return None

    result = payload.get("data", {}).get("result", [])
    if not result:
        return None

    value = result[0].get("value", [None, None])[1]
    return safe_float(value)


@st.cache_data(show_spinner=False, ttl=15)
def query_prometheus_range(
    base_url: str,
    query: str,
    start: datetime,
    end: datetime,
    step: str = "30s",
    refresh_nonce: int = 0,
) -> pd.DataFrame:
    _ = refresh_nonce

    endpoint = f"{base_url.rstrip('/')}/api/v1/query_range"
    params = {
        "query": query,
        "start": start.timestamp(),
        "end": end.timestamp(),
        "step": step,
    }

    try:
        response = requests.get(endpoint, params=params, timeout=5)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return pd.DataFrame(columns=["timestamp", "value"])

    if payload.get("status") != "success":
        return pd.DataFrame(columns=["timestamp", "value"])

    series = payload.get("data", {}).get("result", [])
    if not series:
        return pd.DataFrame(columns=["timestamp", "value"])

    points: List[Dict[str, Any]] = []
    for metric_series in series:
        for raw_ts, raw_value in metric_series.get("values", []):
            numeric_value = safe_float(raw_value)
            if numeric_value is None:
                continue
            points.append(
                {
                    "timestamp": datetime.fromtimestamp(float(raw_ts), tz=timezone.utc),
                    "value": numeric_value,
                }
            )

    if not points:
        return pd.DataFrame(columns=["timestamp", "value"])

    frame = pd.DataFrame(points)
    return frame.groupby("timestamp", as_index=False)["value"].mean().sort_values("timestamp")
