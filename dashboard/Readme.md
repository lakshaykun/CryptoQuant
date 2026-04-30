# CryptoQuant Dashboard

The dashboard is the interactive analytics layer for CryptoQuant. It combines market data, engineered feature data, sentiment data, and model prediction outputs into a single view so you can inspect price action, explain short-term behavior, and evaluate forecast quality from one place.

## What The Dashboard Shows

The interface is organized into six analytics areas.

### 1. Executive Summary

This section gives the quickest read on the current state of the selected symbol.

- Sentiment Score shows the latest aggregated sentiment index.
- Predicted Direction compares the latest forecasted close with the current close.
- Current Close and Predicted Close show the latest observed price and model output.
- Expected Delta shows the implied difference between the prediction and the current market price.

These values are calculated from the latest candles, the latest forecast row, and the most recent sentiment summary.

### 2. Price Action & Forecast

This section combines historical candles with model predictions.

- Price Forecast Extension overlays the forecasted close values on top of recent OHLC candles.
- Sentiment Timeline Overlay on Price shows how the sentiment index evolves alongside price movement.

The chart is driven by candle data, forecast points, and sentiment timeline points fetched for the active symbol and interval.

### 3. Market Dynamics

This section explains the market behavior behind recent price movement.

- Volatility Ribbon + Ratio shows volatility, a short-window volatility baseline, and the volatility ratio.
- Momentum Profile Scatter shows momentum against trend strength.
- Volume Spike Timeline highlights periods where volume spike behavior changes over time.

These views are built from engineered market features such as volatility, volatility_5, volatility_ratio, momentum, trend_strength, and volume_spike.

### 4. Microstructure & Order Flow

This section focuses on lower-level trading structure.

- Order Flow Imbalance shows the balance between buying and selling pressure.
- Candlestick Anatomy shows body size and price range ratio for each candle.
- Log Return Distribution shows the distribution of log returns over the selected window.
- Hourly × Weekday Return Heatmap aggregates average return by hour of day and day of week.

These views help identify intraday patterns, candle structure, return dispersion, and order flow pressure.

### 5. Sentiment Deep Dive

This section breaks sentiment into quality, source mix, and market relationship.

- Avg Confidence shows the weighted average confidence of sentiment observations.
- Message Count shows how many sentiment messages are included in the current window.
- Sentiment per Source compares contribution by source.
- Sentiment–Price Lag Correlation measures how sentiment and log returns move together across a lag range.

The lag chart is especially useful for checking whether sentiment tends to lead price, lag price, or move at the same time.

### 6. Model Evaluation & Raw Data

This section evaluates prediction quality and exposes the underlying rolling values.

- Total Predictions counts stored model outputs.
- Matched (w/ actual) counts prediction rows with actual close values.
- MAE reports mean absolute error over the selected sample.
- Directional Accuracy measures how often the model predicted the right direction.
- Prediction vs Actual Close compares forecasted closes against actual closes.
- Rolling Stats shows the latest candles together with close, volume, and log return values.

## Concepts Used In The Analytics

The dashboard uses a small set of market, sentiment, and prediction concepts repeatedly across the charts.

### Market Concepts

- OHLC candles: open, high, low, close bars for the selected symbol.
- Volume and trades: activity and participation over time.
- Moving averages: short and medium trend references such as MA 5 and MA 20.
- Log return: the percentage-style return measure used for distributions, heatmaps, and correlation work.
- Volatility: rolling price variability.
- Momentum and trend strength: directional market pressure and persistence.
- Order flow imbalance: the relationship between buying and selling pressure.
- Microstructure: candle body size and price range ratio, used to expose fine-grained price structure.

### Sentiment Concepts

- Sentiment index: the aggregated sentiment score for each time window.
- Average confidence: how strongly the sentiment signal is supported.
- Message count: the number of messages contributing to the sentiment window.
- Source breakdown: how sentiment is distributed across sources.
- Engagement: volume-weighted activity behind each source.
- Sentiment-price lag correlation: a cross-correlation view used to inspect whether sentiment leads or follows price.

### Prediction Concepts

- Forecast point: a model output containing predicted close values.
- Predicted close: the model’s estimate of the next close or forecast window close.
- Actual close: the realized market close used for evaluation.
- Error: the gap between forecast and actual values.
- MAE: average absolute forecast error.
- Directional accuracy: the percentage of times the model gets the direction correct.

## How The Analytics Work

The frontend is a React application that queries the dashboard backend through dedicated endpoints for candles, features, sentiment, forecasts, and accuracy metrics. Each chart is tied to a specific query key, so changing the selected symbol, interval, or chart limit triggers a refetch only for the affected panel.

The dashboard uses React Query for caching and auto-refresh. Most panels refresh on a rolling interval, which keeps the display current without manual reloads.

The data flow is straightforward:

1. The user selects a symbol and interval.
2. The dashboard requests the matching candles, market features, sentiment data, or prediction rows.
3. The backend reads from the market, sentiment, and predictions tables.
4. The frontend renders charts, KPIs, and the rolling table from those API responses.

The microstructure heatmap is pre-aggregated on the backend by hour and weekday. The lag correlation view is also computed server-side by aligning sentiment and returns at different lag values.

## Filters And Controls

The dashboard is interactive and all major views can be narrowed with local controls.

- Symbol selector switches the active market pair.
- Interval selector changes the candle timeframe between 1m, 5m, 1h, and 1d.
- Limit inputs control how many rows each chart requests.
- Lag input controls the maximum lag used in sentiment-price correlation.

Because each chart uses its own query key, changing one control only refreshes the panels that depend on it.

## Data Sources

The dashboard consumes:

- market candle data
- engineered market features
- sentiment gold and silver data
- stored prediction rows

These sources power the price, market dynamics, microstructure, sentiment, and evaluation sections.

## Practical Use

Use the dashboard to answer questions such as:

- Is the current market moving with strong momentum or elevated volatility?
- Is sentiment improving before price reacts?
- Which sources dominate the sentiment signal?
- How accurate have the recent predictions been?
- Are there intraday return patterns worth investigating further?

In short, the dashboard is designed to connect the model output to market structure and sentiment context, not just to plot price charts.
