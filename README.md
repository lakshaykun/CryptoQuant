“Designed a modular MLOps system with streaming ingestion, medallion data architecture, automated training pipelines, and real-time model serving.”

Separation of concerns
Real-time + batch architecture
Production readiness

Before training:
    df_train = df.dropna(subset=["target"])
For real-time prediction:
    👉 Keep the last row:
    df_latest = df.orderBy("open_time", ascending=False).limit(1)



the raw data from binance is as follow - 
['open_time',
 'open',
 'high',
 'low',
 'close',
 'volume',
 'close_time',
 'quote_volume',
 'trades',
 'taker_buy_base',
 'taker_buy_quote',
 'ignore']