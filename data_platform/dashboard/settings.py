from pathlib import Path
from typing import Any, Dict

import streamlit as st
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_ROOT = Path(__file__).resolve().parent

DATA_CONFIG_PATH = REPO_ROOT / "configs" / "data.yaml"
MODEL_CONFIG_PATH = REPO_ROOT / "configs" / "model.yaml"
MLFLOW_CONFIG_PATH = REPO_ROOT / "configs" / "mlflow.yaml"

TIME_RANGE_OPTIONS = ["6 hours", "12 hours", "1 day", "7 days", "1 month", "1 year", "Max"]

COLORS = {
    "background":    "#020617",
    "background_soft":"#0B1220",
    "panel":         "rgba(15,23,42,0.78)",
    "panel_strong":  "rgba(15,23,42,0.94)",
    "border":        "rgba(148,163,184,0.20)",
    "grid":          "rgba(148,163,184,0.12)",
    "text":          "#E2E8F0",
    "muted":         "#94A3B8",
    "primary":       "#38BDF8",
    "secondary":     "#14B8A6",
    "accent":        "#A78BFA",
    "success":       "#34D399",
    "warning":       "#F59E0B",
    "danger":        "#FB7185",
}

CHART_COLORS = [
    "#38BDF8","#22D3EE","#14B8A6","#34D399",
    "#A78BFA","#60A5FA","#F472B6","#F59E0B","#FB7185","#818CF8",
]
CHART_TEMPLATE = "plotly_dark"
FONT_FAMILY = "Inter, ui-sans-serif, system-ui, -apple-system, sans-serif"


def apply_page_config() -> None:
    st.set_page_config(
        page_title="CryptoQuant · Model Observatory",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def apply_app_styles() -> None:
    st.markdown("""
    <style>
    :root {
        --bg: #020617; --bg-soft: #0B1220;
        --panel: rgba(15,23,42,0.78); --panel-strong: rgba(15,23,42,0.94);
        --border: rgba(148,163,184,0.20); --text: #E2E8F0; --muted: #94A3B8;
        --primary: #38BDF8; --secondary: #14B8A6;
    }
    .stApp::before {
        content:""; position:fixed; inset:0; z-index:-2;
        background:
            radial-gradient(circle at top left,rgba(56,189,248,0.18),transparent 28%),
            radial-gradient(circle at top right,rgba(20,184,166,0.16),transparent 26%),
            radial-gradient(circle at bottom center,rgba(167,139,250,0.10),transparent 30%),
            linear-gradient(135deg,#020617 0%,#08111F 48%,#020617 100%);
    }
    .stApp::after {
        content:""; position:fixed; inset:0; z-index:-1;
        background-image:linear-gradient(rgba(148,163,184,0.05) 1px,transparent 1px),
            linear-gradient(90deg,rgba(148,163,184,0.05) 1px,transparent 1px);
        background-size:28px 28px;
        mask-image:linear-gradient(to bottom,rgba(0,0,0,0.7),transparent 76%);
        pointer-events:none;
    }
    html,body,.stApp { background:var(--bg); color:var(--text);
        font-family:Inter,ui-sans-serif,system-ui,-apple-system,sans-serif; }
    .stApp { background:transparent; }
    .block-container { max-width:1600px; padding:1.2rem 1.6rem 2rem; }
    div[data-testid="stHorizontalBlock"] { gap:1rem; align-items:stretch; }
    h1,h2,h3,h4,h5,h6,p,span,label,li { color:var(--text); }
    [data-testid="stCaptionContainer"] p { color:var(--muted); }
    div[data-testid="stMetric"] {
        border:1px solid var(--border); border-radius:18px; padding:12px 14px;
        background:linear-gradient(145deg,rgba(15,23,42,0.82),rgba(15,118,110,0.16));
        box-shadow:0 16px 38px rgba(2,6,23,0.35); backdrop-filter:blur(14px);
    }
    div[data-testid="stMetric"] * { color:var(--text) !important; }
    div[data-testid="stMetricLabel"] { color:var(--muted) !important; }
    div[data-testid="stMetricDelta"] { color:var(--primary) !important; }
    section[data-testid="stSidebar"] {
        background:rgba(2,6,23,0.88); backdrop-filter:blur(18px);
        border-right:1px solid var(--border);
    }
    section[data-testid="stSidebar"] * { color:var(--text); }
    div[data-testid="stVerticalBlockBorderWrapper"] {
        border:1px solid var(--border); border-radius:20px;
        background:rgba(15,23,42,0.62); box-shadow:0 20px 55px rgba(2,6,23,0.32);
        backdrop-filter:blur(18px); padding:0.15rem 0.15rem 0.05rem;
    }
    div[data-testid="stDataFrame"],div[data-testid="stTable"] {
        background:rgba(2,6,23,0.40); border:1px solid var(--border);
        border-radius:16px; overflow:hidden;
    }
    div[data-testid="stDataFrame"] * { color:var(--text); }
    div[data-testid="stDataFrame"] thead tr th { background:rgba(15,23,42,0.92); color:var(--muted); }
    div[data-testid="stDataFrame"] tbody tr:nth-child(odd) { background:rgba(15,23,42,0.18); }
    div[data-testid="stAlert"] {
        border-radius:16px; border:1px solid var(--border);
        background:rgba(15,23,42,0.72);
    }
    div.stButton>button,section[data-testid="stSidebar"] button {
        border-radius:12px; border:1px solid rgba(56,189,248,0.30);
        background:linear-gradient(135deg,rgba(56,189,248,0.22),rgba(20,184,166,0.18));
        color:var(--text); box-shadow:0 8px 22px rgba(2,6,23,0.30);
        transition:all 0.15s ease;
    }
    div.stButton>button:hover,section[data-testid="stSidebar"] button:hover {
        border-color:rgba(56,189,248,0.56); transform:translateY(-1px);
    }
    hr { border-color:rgba(148,163,184,0.16); }
    .kpi-label { font-size:0.72rem; text-transform:uppercase;
        letter-spacing:0.12em; color:var(--muted); margin-bottom:2px; }
    .kpi-value { font-size:1.4rem; font-weight:700; color:var(--text); }
    </style>
    """, unsafe_allow_html=True)


@st.cache_data(show_spinner=False)
def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
