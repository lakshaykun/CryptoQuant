"""
charts.py – all Plotly figures for the CryptoQuant observability dashboard.
Every function accepts plain DataFrames and returns a go.Figure – no Streamlit
calls live here so functions stay testable and reusable.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from settings import CHART_COLORS, CHART_TEMPLATE, COLORS, FONT_FAMILY


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _rgba(hex_color: str, alpha: float) -> str:
    v = hex_color.lstrip("#")
    r, g, b = (int(v[i:i+2], 16) for i in (0, 2, 4))
    return f"rgba({r}, {g}, {b}, {alpha})"


def _base_layout(xaxis_title: str, yaxis_title: str, height: int = 340) -> dict:
    axis_common = dict(
        showgrid=True, gridcolor="rgba(148,163,184,0.10)",
        zeroline=True, zerolinecolor="rgba(148,163,184,0.25)", zerolinewidth=1,
        showline=True, linecolor=COLORS["border"],
        ticks="outside", tickfont=dict(color=COLORS["muted"], size=11),
        showspikes=True, spikecolor=COLORS["muted"], spikethickness=1, spikedash="dot",
    )
    return dict(
        template=CHART_TEMPLATE,
        colorway=CHART_COLORS,
        height=height,
        margin=dict(l=28, r=24, t=44, b=28),
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(11,18,32,0.72)",
        font=dict(family=FONT_FAMILY, color=COLORS["text"], size=12),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0,
            bgcolor="rgba(2,6,23,0.55)", bordercolor=COLORS["border"], borderwidth=1,
            font=dict(size=11),
        ),
        hoverlabel=dict(
            bgcolor="rgba(11,18,32,0.95)", bordercolor=COLORS["primary"],
            font=dict(color=COLORS["text"], size=12), namelength=-1,
        ),
        xaxis=dict(title=dict(text=xaxis_title, font=dict(size=12, color=COLORS["muted"])), **axis_common),
        yaxis=dict(title=dict(text=yaxis_title, font=dict(size=12, color=COLORS["muted"])), **axis_common),
    )


def _apply(fig: go.Figure, xaxis_title: str, yaxis_title: str, height: int = 340) -> go.Figure:
    fig.update_layout(**_base_layout(xaxis_title, yaxis_title, height))
    return fig


def _mark_latest(fig: go.Figure, x_val, y_val, color: str, label: str) -> None:
    """Add a diamond marker + annotation at the latest data point."""
    fig.add_trace(go.Scatter(
        x=[x_val], y=[y_val], mode="markers",
        marker=dict(color=color, size=10, symbol="diamond",
                    line=dict(color="white", width=1.5)),
        name=label, showlegend=False,
        hovertemplate=f"{label}: %{{y:.5f}}<extra></extra>",
    ))
    fig.add_annotation(
        x=x_val, y=y_val, text=f" {label}<br><b>{y_val:.5f}</b>",
        showarrow=False, xanchor="left", yanchor="middle",
        font=dict(color=color, size=11),
        bgcolor="rgba(2,6,23,0.7)", borderpad=3,
    )


# ---------------------------------------------------------------------------
# Overview tab
# ---------------------------------------------------------------------------

def plot_ingestion_rate(layer_frame: pd.DataFrame, time_col: str) -> go.Figure:
    rate = (
        layer_frame[[time_col]].dropna()
        .set_index(time_col).resample("1min").size()
        .rename("rpm").reset_index()
    )
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=rate[time_col], y=rate["rpm"], mode="lines",
        line=dict(color=COLORS["primary"], width=2.5, shape="spline", smoothing=1.1),
        fill="tozeroy",
        fillgradient=dict(type="vertical", colorscale=[
            [0, _rgba(COLORS["primary"], 0.0)],
            [1, _rgba(COLORS["primary"], 0.28)],
        ]),
        name="Rows/min",
        hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>Rows/min: <b>%{y:.0f}</b><extra></extra>",
    ))
    if not rate.empty:
        latest = rate.iloc[-1]
        _mark_latest(fig, latest[time_col], float(latest["rpm"]), COLORS["warning"], "Latest")
    return _apply(fig, "Time", "Rows / minute")


def plot_row_count_trend(layer_frame: pd.DataFrame, time_col: str) -> go.Figure:
    trend = (
        layer_frame[[time_col]].dropna()
        .set_index(time_col).resample("5min").size()
        .cumsum().rename("cum").reset_index()
    )
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend[time_col], y=trend["cum"], mode="lines",
        line=dict(color=COLORS["secondary"], width=2.5),
        fill="tozeroy", fillcolor=_rgba(COLORS["secondary"], 0.12),
        name="Cumulative rows",
        hovertemplate="%{x|%H:%M}<br>Cumulative: %{y:.0f}<extra></extra>",
    ))
    return _apply(fig, "Time", "Cumulative rows")


def plot_symbol_distribution(frame: pd.DataFrame) -> go.Figure:
    counts = frame["symbol"].value_counts().reset_index()
    counts.columns = ["symbol", "count"]
    fig = go.Figure(go.Bar(
        x=counts["symbol"], y=counts["count"],
        marker_color=CHART_COLORS[:len(counts)],
        hovertemplate="%{x}<br>Count: %{y}<extra></extra>",
    ))
    fig.update_layout(**_base_layout("Symbol", "Count", 300))
    return fig


def plot_data_latency(frame: pd.DataFrame) -> go.Figure:
    """Plot ingestion_time - open_time latency over time."""
    if "open_time" not in frame.columns or "ingestion_time" not in frame.columns:
        return go.Figure()
    df = frame[["open_time", "ingestion_time"]].dropna().copy()
    df["latency_s"] = (df["ingestion_time"] - df["open_time"]).dt.total_seconds()
    df = df.sort_values("open_time")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["open_time"], y=df["latency_s"], mode="lines",
        line=dict(color=COLORS["warning"], width=2),
        name="Latency (s)",
        hovertemplate="%{x|%H:%M}<br>Latency: %{y:.1f}s<extra></extra>",
    ))
    return _apply(fig, "Time", "Ingestion latency (s)", 280)


# ---------------------------------------------------------------------------
# Model performance tab
# ---------------------------------------------------------------------------

def plot_predicted_vs_actual(frame: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    # Zero reference band
    fig.add_hrect(y0=-0.0001, y1=0.0001, fillcolor=_rgba(COLORS["muted"], 0.06),
                  line_width=0, annotation_text="zero", annotation_position="right")
    fig.add_trace(go.Scatter(
        x=frame["open_time"], y=frame["prediction"],
        mode="lines", line=dict(color=COLORS["primary"], width=2.2, shape="spline", smoothing=0.8),
        fill="tozeroy",
        fillgradient=dict(type="vertical", colorscale=[
            [0, _rgba(COLORS["primary"], 0.0)], [1, _rgba(COLORS["primary"], 0.18)]
        ]),
        name="Predicted",
        hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>Predicted: <b>%{y:.6f}</b><extra></extra>",
    ))
    if "actual_log_return_lead1" in frame.columns:
        fig.add_trace(go.Scatter(
            x=frame["open_time"], y=frame["actual_log_return_lead1"],
            mode="lines", line=dict(color=COLORS["secondary"], width=2, dash="dash", shape="spline"),
            name="Actual",
            hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>Actual: <b>%{y:.6f}</b><extra></extra>",
        ))
    return _apply(fig, "Time", "Log return", 380)


def plot_scatter_pred_vs_actual(frame: pd.DataFrame) -> go.Figure:
    df = frame[["prediction", "actual_log_return_lead1"]].dropna()
    if df.empty:
        return go.Figure()
    lo = min(df["prediction"].min(), df["actual_log_return_lead1"].min())
    hi = max(df["prediction"].max(), df["actual_log_return_lead1"].max())
    # Color by residual magnitude
    residuals = (df["prediction"] - df["actual_log_return_lead1"]).abs()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["actual_log_return_lead1"], y=df["prediction"],
        mode="markers",
        marker=dict(
            color=residuals, colorscale="RdYlGn_r", size=5, opacity=0.65,
            showscale=True, colorbar=dict(title="|error|", thickness=10, len=0.6),
            reversescale=False,
        ),
        name="Pred vs Actual",
        hovertemplate="Actual: <b>%{x:.6f}</b><br>Pred: <b>%{y:.6f}</b><br>|Error|: %{marker.color:.6f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=[lo, hi], y=[lo, hi], mode="lines",
        line=dict(color=COLORS["success"], dash="dash", width=1.5),
        name="Perfect prediction",
    ))
    return _apply(fig, "Actual log return", "Predicted log return", 380)


def plot_residual_distribution(frame: pd.DataFrame) -> go.Figure:
    residuals = frame["residual"].dropna()
    if residuals.empty:
        return go.Figure()
    mu, sigma = residuals.mean(), residuals.std()
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=residuals, nbinsx=60,
        marker=dict(
            color=residuals,
            colorscale=[[0, COLORS["danger"]], [0.5, COLORS["primary"]], [1, COLORS["success"]]],
            opacity=0.82,
        ),
        name="Residuals",
        hovertemplate="Residual: <b>%{x:.6f}</b><br>Count: %{y}<extra></extra>",
    ))
    fig.add_vline(x=0, line_color=COLORS["warning"], line_dash="dash",
                  annotation_text="zero", annotation_position="top right")
    fig.add_vline(x=mu, line_color=COLORS["accent"], line_dash="dot",
                  annotation_text=f"μ={mu:.4f}", annotation_position="top left")
    fig.add_vrect(x0=mu - sigma, x1=mu + sigma,
                  fillcolor=_rgba(COLORS["primary"], 0.06), line_width=0,
                  annotation_text="±1σ", annotation_position="bottom right")
    return _apply(fig, "Residual", "Count", 320)


def plot_residuals(frame: pd.DataFrame) -> go.Figure:
    residuals = frame["residual"].fillna(0.0)
    colors = [COLORS["success"] if v >= 0 else COLORS["danger"] for v in residuals]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=frame["open_time"], y=residuals, mode="markers",
        marker=dict(size=6, color=colors, opacity=0.85),
        name="Residual",
        hovertemplate="%{x|%H:%M}<br>Residual: %{y:.6f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_color=COLORS["muted"], line_dash="dash")
    return _apply(fig, "Time", "Prediction error")


def plot_rolling_rmse(frame: pd.DataFrame) -> go.Figure:
    df = frame[["open_time", "residual"]].copy()
    w = max(20, min(120, len(df) // 6))
    df["rolling_rmse"] = df["residual"].pow(2).rolling(w, min_periods=max(5, w//4)).mean().pow(0.5)
    overall_rmse = float(np.sqrt((df["residual"] ** 2).mean())) if not df.empty else None
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["open_time"], y=df["rolling_rmse"], mode="lines",
        line=dict(color=COLORS["danger"], width=2.5, shape="spline", smoothing=0.8),
        fill="tozeroy",
        fillgradient=dict(type="vertical", colorscale=[
            [0, _rgba(COLORS["danger"], 0.0)], [1, _rgba(COLORS["danger"], 0.20)]
        ]),
        name=f"Rolling RMSE (w={w})",
        hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>Rolling RMSE: <b>%{y:.6f}</b><extra></extra>",
    ))
    if overall_rmse is not None:
        fig.add_hline(y=overall_rmse, line_color=COLORS["warning"], line_dash="dot",
                      annotation_text=f"Overall RMSE: {overall_rmse:.5f}",
                      annotation_position="right")
    valid = df.dropna(subset=["rolling_rmse"])
    if not valid.empty:
        row = valid.iloc[-1]
        _mark_latest(fig, row["open_time"], float(row["rolling_rmse"]), COLORS["warning"], "Latest RMSE")
    return _apply(fig, "Time", "RMSE")


def plot_rolling_f1(frame: pd.DataFrame, col: str = "f1_sign_short") -> go.Figure:
    if col not in frame.columns:
        return go.Figure()
    df = frame[["open_time", col]].dropna().copy()
    w = max(20, min(120, len(df) // 6))
    df["rolling_f1"] = df[col].rolling(w, min_periods=max(5, w//4)).mean()
    fig = go.Figure()
    # Good / bad bands
    fig.add_hrect(y0=0.6, y1=1.0, fillcolor=_rgba(COLORS["success"], 0.06),
                  line_width=0, annotation_text="Good (>0.6)", annotation_position="top right")
    fig.add_hrect(y0=0.0, y1=0.4, fillcolor=_rgba(COLORS["danger"], 0.06),
                  line_width=0, annotation_text="Poor (<0.4)", annotation_position="bottom right")
    fig.add_trace(go.Scatter(
        x=df["open_time"], y=df["rolling_f1"], mode="lines",
        line=dict(color=COLORS["accent"], width=2.5, shape="spline", smoothing=0.9),
        fill="tozeroy",
        fillgradient=dict(type="vertical", colorscale=[
            [0, _rgba(COLORS["accent"], 0.0)], [1, _rgba(COLORS["accent"], 0.18)]
        ]),
        name=f"Rolling F1 (w={w})",
        hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>F1: <b>%{y:.4f}</b><extra></extra>",
    ))
    fig.add_hline(y=0.5, line_color=COLORS["muted"], line_dash="dot",
                  annotation_text="Random baseline (0.5)", annotation_position="right")
    valid = df.dropna(subset=["rolling_f1"])
    if not valid.empty:
        row = valid.iloc[-1]
        _mark_latest(fig, row["open_time"], float(row["rolling_f1"]), COLORS["accent"], "Latest F1")
    return _apply(fig, "Time", "F1 score")


def plot_confusion_matrix(frame: pd.DataFrame, pred_col: str, actual_col: str) -> go.Figure:
    if pred_col not in frame.columns or actual_col not in frame.columns:
        return go.Figure()
    df = frame[[pred_col, actual_col]].dropna()
    if df.empty:
        return go.Figure()
    labels = sorted(df[actual_col].unique())
    matrix = pd.crosstab(df[actual_col], df[pred_col].round().astype(int))
    z = matrix.values.tolist()
    x = [str(c) for c in matrix.columns]
    y = [str(i) for i in matrix.index]
    fig = go.Figure(go.Heatmap(
        z=z, x=x, y=y,
        colorscale=[[0, "#020617"], [1, COLORS["primary"]]],
        showscale=True, text=z,
        texttemplate="%{text}",
        hovertemplate="Actual: %{y}<br>Predicted: %{x}<br>Count: %{z}<extra></extra>",
    ))
    return _apply(fig, "Predicted", "Actual", 320)


def plot_close_price_comparison(frame: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if "actual_close" in frame.columns:
        fig.add_trace(go.Scatter(
            x=frame["open_time"], y=frame["actual_close"],
            mode="lines", line=dict(color=COLORS["secondary"], width=2, dash="dash"),
            name="Actual close",
        ))
    if "predicted_close" in frame.columns:
        fig.add_trace(go.Scatter(
            x=frame["open_time"], y=frame["predicted_close"],
            mode="lines", line=dict(color=COLORS["primary"], width=2.5),
            fill="tozeroy", fillcolor=_rgba(COLORS["primary"], 0.08),
            name="Predicted close",
        ))
    return _apply(fig, "Time", "Close price (USDT)")


# ---------------------------------------------------------------------------
# Prediction analysis tab
# ---------------------------------------------------------------------------

def plot_prediction_distribution(frame: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if "prediction" in frame.columns:
        fig.add_trace(go.Histogram(
            x=frame["prediction"].dropna(), nbinsx=60,
            marker_color=_rgba(COLORS["primary"], 0.7),
            name="Predicted",
        ))
    if "actual_log_return_lead1" in frame.columns:
        fig.add_trace(go.Histogram(
            x=frame["actual_log_return_lead1"].dropna(), nbinsx=60,
            marker_color=_rgba(COLORS["secondary"], 0.6),
            name="Actual",
        ))
    fig.update_layout(barmode="overlay")
    return _apply(fig, "Log return", "Count", 320)


def plot_confidence_vs_accuracy(frame: pd.DataFrame, threshold: float) -> go.Figure:
    df = frame[["prediction", "direction_correct"]].dropna().copy()
    if df.empty:
        return go.Figure()
    df["abs_pred"] = df["prediction"].abs()
    bins = pd.cut(df["abs_pred"], bins=10)
    grp = df.groupby(bins, observed=True).agg(accuracy=("direction_correct", "mean"), count=("direction_correct", "size")).reset_index()
    grp["midpoint"] = grp["abs_pred"].apply(lambda b: b.mid)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=grp["midpoint"].astype(float), y=grp["accuracy"],
        marker=dict(
            color=grp["accuracy"],
            colorscale=[[0, COLORS["danger"]], [0.5, COLORS["warning"]], [1, COLORS["success"]]],
            opacity=0.85,
        ),
        text=[f"{v:.1%}" for v in grp["accuracy"]],
        textposition="outside",
        name="Dir. accuracy",
        hovertemplate="Pred magnitude: <b>%{x:.4f}</b><br>Accuracy: <b>%{y:.2%}</b><extra></extra>",
    ))
    fig.add_vline(x=threshold, line_color=COLORS["warning"], line_dash="dash",
                  annotation_text=f"threshold={threshold}", annotation_position="top right")
    fig.add_hline(y=0.5, line_color=COLORS["muted"], line_dash="dot",
                  annotation_text="random", annotation_position="right")
    fig.update_yaxes(tickformat=".0%", range=[0, 1.1])
    return _apply(fig, "Prediction magnitude", "Directional accuracy", 340)


def plot_directional_accuracy_by_horizon(frame: pd.DataFrame) -> go.Figure:
    rows = []
    for col, label in [("return_short", "Short"), ("return_long", "Long")]:
        if col in frame.columns and "actual_log_return_lead1" in frame.columns:
            df = frame[[col, "actual_log_return_lead1"]].dropna()
            if not df.empty:
                acc = (np.sign(df[col]) == np.sign(df["actual_log_return_lead1"])).mean()
                rows.append({"Horizon": label, "Dir. Accuracy": acc})
    if not rows:
        return go.Figure()
    df_out = pd.DataFrame(rows)
    fig = go.Figure(go.Bar(
        x=df_out["Horizon"], y=df_out["Dir. Accuracy"],
        marker_color=[COLORS["primary"], COLORS["secondary"]],
        text=[f"{v:.2%}" for v in df_out["Dir. Accuracy"]],
        textposition="outside",
        hovertemplate="%{x}<br>Accuracy: <b>%{y:.2%}</b><extra></extra>",
    ))
    fig.add_hline(y=0.5, line_color=COLORS["warning"], line_dash="dot",
                  annotation_text="50% baseline", annotation_position="right")
    fig.update_yaxes(tickformat=".0%", range=[0, 1.1])
    return _apply(fig, "Horizon", "Directional accuracy", 320)


def plot_pnl_curve(cum_return_series: pd.Series) -> go.Figure:
    vals = cum_return_series.values
    times = cum_return_series.index
    # Running max for drawdown shading
    running_max = np.maximum.accumulate(np.where(np.isnan(vals), 0, vals))
    drawdown = vals - running_max
    line_color = COLORS["success"] if (len(vals) == 0 or vals[-1] >= 0) else COLORS["danger"]
    fig = go.Figure()
    # Drawdown fill
    fig.add_trace(go.Scatter(
        x=times, y=drawdown, mode="lines",
        line=dict(color=_rgba(COLORS["danger"], 0.0), width=0),
        fill="tozeroy", fillcolor=_rgba(COLORS["danger"], 0.15),
        name="Drawdown",
        hovertemplate="<b>%{x|%H:%M}</b><br>Drawdown: <b>%{y:.4f}</b><extra></extra>",
    ))
    # PnL line
    fig.add_trace(go.Scatter(
        x=times, y=vals,
        mode="lines", line=dict(color=line_color, width=2.5, shape="spline", smoothing=0.7),
        fill="tozeroy",
        fillgradient=dict(type="vertical", colorscale=[
            [0, _rgba(line_color, 0.0)], [1, _rgba(line_color, 0.22)]
        ]),
        name="Cumulative return",
        hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>Cum. return: <b>%{y:.4f}</b><extra></extra>",
    ))
    fig.add_hline(y=0, line_color=COLORS["muted"], line_dash="dash")
    if len(vals) > 0 and not np.isnan(vals[-1]):
        _mark_latest(fig, times[-1], float(vals[-1]), line_color, "Final")
    return _apply(fig, "Time", "Cumulative log return", 360)


# ---------------------------------------------------------------------------
# Feature & data diagnostics tab
# ---------------------------------------------------------------------------

def plot_feature_histogram_overlay(
    live: pd.Series,
    baseline_stats: Optional[pd.DataFrame],
    feature: str,
) -> go.Figure:
    fig = go.Figure()
    live_vals = live.dropna()
    if not live_vals.empty:
        fig.add_trace(go.Histogram(
            x=live_vals, nbinsx=50,
            marker_color=_rgba(COLORS["primary"], 0.7),
            name="Live",
        ))
    if baseline_stats is not None and feature in baseline_stats.index:
        row = baseline_stats.loc[feature]
        mu, sigma = float(row["mean"]), float(row["std"])
        if sigma > 0:
            x_range = np.linspace(mu - 4*sigma, mu + 4*sigma, 200)
            y_gauss = (1 / (sigma * np.sqrt(2 * np.pi))) * np.exp(-0.5 * ((x_range - mu) / sigma) ** 2)
            # Scale to approximate histogram counts
            scale = len(live_vals) * (live_vals.max() - live_vals.min()) / 50 if not live_vals.empty else 1
            fig.add_trace(go.Scatter(
                x=x_range, y=y_gauss * scale,
                mode="lines", line=dict(color=COLORS["warning"], width=2, dash="dash"),
                name="Training dist.",
            ))
    fig.update_layout(barmode="overlay", title_text=feature, title_font_size=13)
    return _apply(fig, feature, "Count", 260)


def plot_correlation_heatmap(frame: pd.DataFrame, target: str, features: List[str]) -> go.Figure:
    cols = [f for f in features if f in frame.columns] + ([target] if target in frame.columns else [])
    if len(cols) < 2:
        return go.Figure()
    corr = frame[cols].dropna().corr()
    # Highlight target row/column
    fig = go.Figure(go.Heatmap(
        z=corr.values,
        x=corr.columns.tolist(),
        y=corr.index.tolist(),
        colorscale="RdBu",
        zmid=0,
        text=np.round(corr.values, 2),
        texttemplate="%{text}",
        hovertemplate="X: %{x}<br>Y: %{y}<br>Corr: %{z:.3f}<extra></extra>",
    ))
    return _apply(fig, "", "", max(400, 30 * len(cols)))


def plot_feature_importance(importance_dict: Dict[str, float], top_n: int = 20) -> go.Figure:
    if not importance_dict:
        return go.Figure()
    df = pd.DataFrame(list(importance_dict.items()), columns=["feature", "importance"])
    df = df.nlargest(top_n, "importance").sort_values("importance")
    fig = go.Figure(go.Bar(
        y=df["feature"], x=df["importance"],
        orientation="h",
        marker_color=COLORS["accent"],
        hovertemplate="%{y}<br>Importance: %{x:.4f}<extra></extra>",
    ))
    return _apply(fig, "Importance", "Feature", max(360, 22 * top_n))


def plot_null_rates(frame: pd.DataFrame) -> go.Figure:
    null_pct = (frame.isnull().mean() * 100).sort_values(ascending=False)
    null_pct = null_pct[null_pct > 0]
    if null_pct.empty:
        return go.Figure()
    colors = [COLORS["danger"] if v > 10 else COLORS["warning"] if v > 1 else COLORS["success"]
              for v in null_pct.values]
    fig = go.Figure(go.Bar(
        x=null_pct.index, y=null_pct.values,
        marker_color=colors,
        hovertemplate="%{x}<br>Null%: %{y:.2f}%<extra></extra>",
    ))
    return _apply(fig, "Column", "Null %", 320)


# ---------------------------------------------------------------------------
# Drift & Retraining tab
# ---------------------------------------------------------------------------

def plot_drift_scores(frame: pd.DataFrame, data_threshold: float, model_threshold: float) -> go.Figure:
    fig = go.Figure()
    model_col = "model_drift_score" if "model_drift_score" in frame.columns else "prediction_drift_score"
    for col, color, dash, label in [
        ("drift_score", COLORS["primary"], "solid", "Overall drift"),
        ("data_drift_score", COLORS["secondary"], "dash", "Data drift"),
        (model_col, COLORS["accent"], "dot", "Model drift"),
    ]:
        if col in frame.columns and frame[col].notna().any():
            fig.add_trace(go.Scatter(
                x=frame["event_time"], y=frame[col],
                mode="lines", line=dict(color=color, width=2.2, dash=dash),
                name=label,
                hovertemplate=f"%{{x|%H:%M}}<br>{label}: %{{y:.4f}}<extra></extra>",
            ))
    if "triggered" in frame.columns:
        triggered = frame[frame["triggered"].fillna(False).astype(bool)]
        if not triggered.empty and "drift_score" in triggered.columns:
            fig.add_trace(go.Scatter(
                x=triggered["event_time"], y=triggered["drift_score"],
                mode="markers", marker=dict(color=COLORS["warning"], size=10, symbol="diamond"),
                name="Retrain triggered",
                hovertemplate="%{x|%H:%M}<br>Trigger<extra></extra>",
            ))
    fig.add_hline(y=data_threshold, line_color=COLORS["warning"], line_dash="dash",
                  annotation_text=f"Data threshold {data_threshold}")
    fig.add_hline(y=model_threshold, line_color=COLORS["danger"], line_dash="dash",
                  annotation_text=f"Model threshold {model_threshold}")
    return _apply(fig, "Event time", "Drift score", 360)


def plot_prediction_shift(before: pd.Series, after: pd.Series) -> go.Figure:
    """Compare prediction distribution before vs after a retraining event."""
    fig = go.Figure()
    for series, name, color in [(before, "Pre-retrain", COLORS["danger"]), (after, "Post-retrain", COLORS["success"])]:
        if not series.dropna().empty:
            fig.add_trace(go.Histogram(
                x=series.dropna(), nbinsx=50,
                marker_color=_rgba(color, 0.65), name=name,
            ))
    fig.update_layout(barmode="overlay")
    return _apply(fig, "Predicted return", "Count", 300)


def plot_regime_performance(regime_df: pd.DataFrame, regime_col: str) -> go.Figure:
    if regime_df.empty:
        return go.Figure()
    fig = make_subplots(rows=1, cols=2, subplot_titles=["Directional Accuracy", "MAE"])
    fig.add_trace(go.Bar(
        x=regime_df[regime_col].astype(str), y=regime_df["dir_accuracy"],
        marker_color=COLORS["primary"], name="Dir. Accuracy",
    ), row=1, col=1)
    fig.add_trace(go.Bar(
        x=regime_df[regime_col].astype(str), y=regime_df["mae"],
        marker_color=COLORS["danger"], name="MAE",
    ), row=1, col=2)
    fig.update_layout(**_base_layout("Regime", "", 320))
    return fig
