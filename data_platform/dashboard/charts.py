from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from settings import CHART_COLORS, CHART_TEMPLATE, COLORS, FONT_FAMILY


def _rgba(hex_color: str, alpha: float) -> str:
    value = hex_color.lstrip("#")
    red, green, blue = (int(value[index : index + 2], 16) for index in (0, 2, 4))
    return f"rgba({red}, {green}, {blue}, {alpha})"


def apply_plot_style(fig: go.Figure, xaxis_title: str, yaxis_title: str, height: int = 320) -> go.Figure:
    fig.update_layout(
        template=CHART_TEMPLATE,
        colorway=CHART_COLORS,
        margin=dict(l=24, r=20, t=40, b=24),
        hovermode="x unified",
        height=height,
        paper_bgcolor="rgba(0, 0, 0, 0)",
        plot_bgcolor="rgba(15, 23, 42, 0.52)",
        font=dict(family=FONT_FAMILY, color=COLORS["text"]),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1.0,
            bgcolor="rgba(2, 6, 23, 0.42)",
            bordercolor=COLORS["border"],
            borderwidth=1,
        ),
        hoverlabel=dict(
            bgcolor=COLORS["panel_strong"],
            bordercolor=COLORS["border"],
            font=dict(color=COLORS["text"]),
        ),
    )
    fig.update_xaxes(
        title=xaxis_title,
        showgrid=True,
        gridcolor=COLORS["grid"],
        zeroline=False,
        showline=True,
        linecolor=COLORS["border"],
        ticks="outside",
        tickfont=dict(color=COLORS["muted"]),
    )
    fig.update_yaxes(
        title=yaxis_title,
        showgrid=True,
        gridcolor=COLORS["grid"],
        zeroline=False,
        showline=True,
        linecolor=COLORS["border"],
        ticks="outside",
        tickfont=dict(color=COLORS["muted"]),
    )
    return fig


def plot_ingestion_rate(layer_frame: pd.DataFrame, time_col: str) -> go.Figure:
    rate_frame = (
        layer_frame[[time_col]]
        .dropna()
        .set_index(time_col)
        .resample("1min")
        .size()
        .rename("rows_per_minute")
        .reset_index()
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=rate_frame[time_col],
            y=rate_frame["rows_per_minute"],
            mode="lines",
            line=dict(color=COLORS["primary"], width=2.7),
            fill="tozeroy",
            fillcolor=_rgba(COLORS["primary"], 0.16),
            line_shape="spline",
            name="Ingestion rate",
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Rows/min: %{y:.0f}<extra></extra>",
        )
    )
    return apply_plot_style(fig, "Time", "Rows / minute")


def plot_row_count_trend(layer_frame: pd.DataFrame, time_col: str) -> go.Figure:
    trend_frame = (
        layer_frame[[time_col]]
        .dropna()
        .set_index(time_col)
        .resample("5min")
        .size()
        .rename("rows")
        .cumsum()
        .rename("cumulative_rows")
        .reset_index()
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=trend_frame[time_col],
            y=trend_frame["cumulative_rows"],
            mode="lines",
            line=dict(color=COLORS["secondary"], width=2.7),
            fill="tozeroy",
            fillcolor=_rgba(COLORS["secondary"], 0.12),
            line_shape="spline",
            name="Cumulative rows",
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Cumulative rows: %{y:.0f}<extra></extra>",
        )
    )
    return apply_plot_style(fig, "Time", "Rows")


def plot_predicted_vs_actual(frame: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=frame["open_time"],
            y=frame["prediction"],
            mode="lines",
            line=dict(color=COLORS["primary"], width=2.7),
            fill="tozeroy",
            fillcolor=_rgba(COLORS["primary"], 0.12),
            line_shape="spline",
            name="Prediction",
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Prediction: %{y:.6f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=frame["open_time"],
            y=frame["actual_log_return_lead1"],
            mode="lines",
            line=dict(color=COLORS["secondary"], width=2.2, dash="dash"),
            line_shape="spline",
            name="Actual",
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Actual: %{y:.6f}<extra></extra>",
        )
    )
    return apply_plot_style(fig, "Time", "Log return")


def plot_close_price_comparison(frame: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=frame["open_time"],
            y=frame["actual_close"],
            mode="lines",
            line=dict(color=COLORS["secondary"], width=2.2, dash="dash"),
            line_shape="spline",
            name="Actual close",
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Actual close: %{y:.4f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=frame["open_time"],
            y=frame["predicted_close"],
            mode="lines",
            line=dict(color=COLORS["primary"], width=2.8),
            fill="tozeroy",
            fillcolor=_rgba(COLORS["primary"], 0.10),
            line_shape="spline",
            name="Predicted close",
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Predicted close: %{y:.4f}<extra></extra>",
        )
    )
    return apply_plot_style(fig, "Time", "Close price (USDT)")


def plot_residuals(frame: pd.DataFrame) -> go.Figure:
    residuals = frame["residual"].fillna(0.0)
    marker_colors = [COLORS["success"] if value >= 0 else COLORS["danger"] for value in residuals]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=frame["open_time"],
            y=residuals,
            mode="markers",
            marker=dict(size=7, color=marker_colors, opacity=0.9, line=dict(color="rgba(255, 255, 255, 0.35)", width=0.5)),
            name="Residual",
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Residual: %{y:.6f}<extra></extra>",
        )
    )
    fig.add_hline(y=0.0, line_color=COLORS["muted"], line_dash="dash")
    return apply_plot_style(fig, "Time", "Prediction error")


def plot_rolling_rmse(frame: pd.DataFrame) -> go.Figure:
    rmse_frame = frame[["open_time", "residual"]].copy()

    window = max(20, min(120, max(20, len(rmse_frame) // 6)))
    rmse_frame["rolling_rmse"] = (
        rmse_frame["residual"].pow(2).rolling(window=window, min_periods=max(5, window // 4)).mean().pow(0.5)
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=rmse_frame["open_time"],
            y=rmse_frame["rolling_rmse"],
            mode="lines",
            line=dict(color=COLORS["primary"], width=2.7),
            fill="tozeroy",
            fillcolor=_rgba(COLORS["primary"], 0.12),
            line_shape="spline",
            name=f"Rolling RMSE ({window})",
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Rolling RMSE: %{y:.6f}<extra></extra>",
        )
    )

    latest_valid = rmse_frame.dropna(subset=["rolling_rmse"])
    if not latest_valid.empty:
        latest = latest_valid.iloc[-1]
        fig.add_trace(
            go.Scatter(
                x=[latest["open_time"]],
                y=[latest["rolling_rmse"]],
                mode="markers",
                marker=dict(color=COLORS["warning"], size=9, symbol="diamond"),
                name="Latest RMSE",
                showlegend=False,
                hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Latest RMSE: %{y:.6f}<extra></extra>",
            )
        )

    return apply_plot_style(fig, "Time", "RMSE")


def plot_drift_scores(frame: pd.DataFrame, data_threshold: float, prediction_threshold: float) -> go.Figure:
    fig = go.Figure()

    if "drift_score" in frame.columns and frame["drift_score"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=frame["event_time"],
                y=frame["drift_score"],
                mode="lines",
                line=dict(color=COLORS["primary"], width=2.8),
                fill="tozeroy",
                fillcolor=_rgba(COLORS["primary"], 0.12),
                line_shape="spline",
                name="Overall drift",
                hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Overall drift: %{y:.4f}<extra></extra>",
            )
        )

    if "data_drift_score" in frame.columns and frame["data_drift_score"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=frame["event_time"],
                y=frame["data_drift_score"],
                mode="lines",
                line=dict(color=COLORS["secondary"], width=2.2, dash="dash"),
                line_shape="spline",
                name="Data drift",
                hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Data drift: %{y:.4f}<extra></extra>",
            )
        )

    if "prediction_drift_score" in frame.columns and frame["prediction_drift_score"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=frame["event_time"],
                y=frame["prediction_drift_score"],
                mode="lines",
                line=dict(color=COLORS["accent"], width=2.2, dash="dot"),
                line_shape="spline",
                name="Prediction drift",
                hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Prediction drift: %{y:.4f}<extra></extra>",
            )
        )

    if "triggered" in frame.columns:
        triggered = frame[frame["triggered"].fillna(False).astype(bool)]
        if not triggered.empty:
            fig.add_trace(
                go.Scatter(
                    x=triggered["event_time"],
                    y=triggered["drift_score"],
                    mode="markers",
                    marker=dict(color=COLORS["warning"], size=9, symbol="diamond"),
                    name="Retraining trigger",
                    customdata=triggered["trigger_reason"] if "trigger_reason" in triggered.columns else None,
                    hovertemplate=(
                        "%{x|%Y-%m-%d %H:%M}<br>Trigger drift: %{y:.4f}"
                        + ("<br>Reason: %{customdata}" if "trigger_reason" in triggered.columns else "")
                        + "<extra></extra>"
                    ),
                )
            )

    fig.add_hline(y=data_threshold, line_color=COLORS["warning"], line_dash="dash")
    fig.add_hline(y=prediction_threshold, line_color=COLORS["danger"], line_dash="dash")
    return apply_plot_style(fig, "Event time", "Drift score")


def plot_prometheus_series(frame: pd.DataFrame, label: str, y_title: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=frame["timestamp"],
            y=frame["value"],
            mode="lines",
            line=dict(color=COLORS["primary"], width=2.7),
            fill="tozeroy",
            fillcolor=_rgba(COLORS["primary"], 0.12),
            line_shape="spline",
            name=label,
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>%{y:.6f}<extra></extra>",
        )
    )
    if not frame.empty:
        latest = frame.iloc[-1]
        fig.add_trace(
            go.Scatter(
                x=[latest["timestamp"]],
                y=[latest["value"]],
                mode="markers",
                marker=dict(color=COLORS["accent"], size=8, symbol="circle"),
                name=f"Latest {label}",
                showlegend=False,
                hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Latest value: %{y:.6f}<extra></extra>",
            )
        )
    return apply_plot_style(fig, "Time", y_title)
