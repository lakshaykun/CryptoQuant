from pathlib import Path
from typing import Any, Dict

import streamlit as st
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_ROOT = Path(__file__).resolve().parent

DATA_CONFIG_PATH = REPO_ROOT / "configs" / "data.yaml"
MODEL_CONFIG_PATH = REPO_ROOT / "configs" / "model.yaml"
MLFLOW_CONFIG_PATH = REPO_ROOT / "configs" / "mlflow.yaml"

AUTO_REFRESH_SECONDS = 30

TIME_RANGE_OPTIONS = ["Last 15 min", "1 hour", "6 hours", "24 hours", "Custom range"]

COLORS = {
    "background": "#020617",
    "background_soft": "#0B1220",
    "panel": "rgba(15, 23, 42, 0.78)",
    "panel_strong": "rgba(15, 23, 42, 0.94)",
    "border": "rgba(148, 163, 184, 0.20)",
    "grid": "rgba(148, 163, 184, 0.12)",
    "text": "#E2E8F0",
    "muted": "#94A3B8",
    "primary": "#38BDF8",
    "secondary": "#14B8A6",
    "accent": "#A78BFA",
    "success": "#34D399",
    "warning": "#F59E0B",
    "danger": "#FB7185",
}

CHART_COLORS = [
    "#38BDF8",
    "#22D3EE",
    "#14B8A6",
    "#34D399",
    "#A78BFA",
    "#60A5FA",
    "#F472B6",
    "#F59E0B",
    "#FB7185",
    "#818CF8",
]
CHART_TEMPLATE = "plotly_dark"
FONT_FAMILY = "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"


def apply_page_config() -> None:
    st.set_page_config(
        page_title="CryptoQuant MLOps Console",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def apply_app_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --dashboard-bg: #020617;
            --dashboard-bg-soft: #0B1220;
            --dashboard-panel: rgba(15, 23, 42, 0.78);
            --dashboard-panel-strong: rgba(15, 23, 42, 0.94);
            --dashboard-border: rgba(148, 163, 184, 0.20);
            --dashboard-text: #E2E8F0;
            --dashboard-muted: #94A3B8;
            --dashboard-primary: #38BDF8;
            --dashboard-secondary: #14B8A6;
        }

        .stApp::before {
            content: "";
            position: fixed;
            inset: 0;
            z-index: -2;
            background:
                radial-gradient(circle at top left, rgba(56, 189, 248, 0.18), transparent 28%),
                radial-gradient(circle at top right, rgba(20, 184, 166, 0.16), transparent 26%),
                radial-gradient(circle at bottom center, rgba(167, 139, 250, 0.10), transparent 30%),
                linear-gradient(135deg, #020617 0%, #08111F 48%, #020617 100%);
        }

        .stApp::after {
            content: "";
            position: fixed;
            inset: 0;
            z-index: -1;
            background-image:
                linear-gradient(rgba(148, 163, 184, 0.05) 1px, transparent 1px),
                linear-gradient(90deg, rgba(148, 163, 184, 0.05) 1px, transparent 1px);
            background-size: 28px 28px;
            mask-image: linear-gradient(to bottom, rgba(0, 0, 0, 0.7), transparent 76%);
            pointer-events: none;
        }

        html, body, .stApp {
            background: var(--dashboard-bg);
            color: var(--dashboard-text);
            font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif;
        }

        .stApp {
            background: transparent;
        }

        .block-container {
            max-width: 1500px;
            padding-top: 1.2rem;
            padding-bottom: 2rem;
            padding-left: 1.6rem;
            padding-right: 1.6rem;
        }

        div[data-testid="stHorizontalBlock"] {
            gap: 1rem;
            align-items: stretch;
        }

        h1, h2, h3, h4, h5, h6, p, span, label, li {
            color: var(--dashboard-text);
        }

        [data-testid="stCaptionContainer"] p {
            color: var(--dashboard-muted);
        }

        .hero-card {
            position: relative;
            overflow: hidden;
            padding: 1.25rem 1.35rem;
            border-radius: 28px;
            border: 1px solid var(--dashboard-border);
            background: linear-gradient(135deg, rgba(15, 23, 42, 0.96), rgba(15, 118, 110, 0.18) 52%, rgba(2, 6, 23, 0.96));
            box-shadow: 0 32px 90px rgba(2, 6, 23, 0.55);
            backdrop-filter: blur(18px);
        }

        .hero-card::after {
            content: "";
            position: absolute;
            inset: -18% -12% auto auto;
            width: 280px;
            height: 280px;
            background: radial-gradient(circle, rgba(56, 189, 248, 0.24), transparent 68%);
            pointer-events: none;
        }

        .hero-eyebrow {
            position: relative;
            z-index: 1;
            display: inline-block;
            margin-bottom: 0.25rem;
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-size: 0.72rem;
            color: var(--dashboard-muted);
        }

        .hero-title {
            position: relative;
            z-index: 1;
            margin: 0.2rem 0 0.45rem;
            font-size: clamp(1.9rem, 3vw, 3rem);
            line-height: 1.05;
            letter-spacing: -0.04em;
            color: var(--dashboard-text);
        }

        .hero-copy {
            position: relative;
            z-index: 1;
            margin: 0;
            max-width: 82ch;
            color: var(--dashboard-muted);
            font-size: 0.98rem;
        }

        .hero-meta {
            position: relative;
            z-index: 1;
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            margin-top: 0.9rem;
        }

        .hero-pill {
            padding: 0.45rem 0.8rem;
            border-radius: 999px;
            border: 1px solid rgba(148, 163, 184, 0.22);
            background: rgba(15, 23, 42, 0.72);
            color: var(--dashboard-text);
            font-size: 0.84rem;
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
        }

        h1 {
            letter-spacing: -0.03em;
            font-weight: 700;
        }

        h2, h3 {
            letter-spacing: -0.02em;
            font-weight: 650;
        }

        div[data-testid="stMetric"] {
            border: 1px solid var(--dashboard-border);
            border-radius: 18px;
            padding: 12px 14px;
            background: linear-gradient(145deg, rgba(15, 23, 42, 0.82), rgba(15, 118, 110, 0.16));
            box-shadow: 0 16px 38px rgba(2, 6, 23, 0.35);
            backdrop-filter: blur(14px);
        }

        div[data-testid="stMetric"] * {
            color: var(--dashboard-text) !important;
        }

        div[data-testid="stMetricValue"] {
            color: var(--dashboard-text) !important;
        }

        div[data-testid="stMetricLabel"] {
            color: var(--dashboard-muted) !important;
        }

        div[data-testid="stMetricDelta"] {
            color: var(--dashboard-primary) !important;
        }

        section[data-testid="stSidebar"] {
            background: rgba(2, 6, 23, 0.88);
            backdrop-filter: blur(18px);
            border-right: 1px solid var(--dashboard-border);
        }

        section[data-testid="stSidebar"] * {
            color: var(--dashboard-text);
        }

        div[data-testid="stVerticalBlockBorderWrapper"] {
            border: 1px solid var(--dashboard-border);
            border-radius: 20px;
            background: rgba(15, 23, 42, 0.62);
            box-shadow: 0 20px 55px rgba(2, 6, 23, 0.32);
            backdrop-filter: blur(18px);
            padding: 0.15rem 0.15rem 0.05rem;
        }

        div[data-testid="stDataFrame"], div[data-testid="stTable"] {
            background: rgba(2, 6, 23, 0.40);
            border: 1px solid var(--dashboard-border);
            border-radius: 16px;
            overflow: hidden;
        }

        div[data-testid="stDataFrame"] {
            padding: 0.15rem;
        }

        div[data-testid="stDataFrame"] * {
            color: var(--dashboard-text);
        }

        div[data-testid="stDataFrame"] thead tr th {
            background: rgba(15, 23, 42, 0.92);
            color: var(--dashboard-muted);
        }

        div[data-testid="stDataFrame"] tbody tr:nth-child(odd) {
            background: rgba(15, 23, 42, 0.18);
        }

        div[data-testid="stAlert"] {
            border-radius: 16px;
            border: 1px solid var(--dashboard-border);
            background: rgba(15, 23, 42, 0.72);
        }

        div.stButton > button, section[data-testid="stSidebar"] button {
            border-radius: 12px;
            border: 1px solid rgba(56, 189, 248, 0.30);
            background: linear-gradient(135deg, rgba(56, 189, 248, 0.22), rgba(20, 184, 166, 0.18));
            color: var(--dashboard-text);
            box-shadow: 0 8px 22px rgba(2, 6, 23, 0.30);
        }

        div.stButton > button:hover, section[data-testid="stSidebar"] button:hover {
            border-color: rgba(56, 189, 248, 0.56);
            transform: translateY(-1px);
        }

        hr {
            border-color: rgba(148, 163, 184, 0.16);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}
