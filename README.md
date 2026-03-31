“Designed a modular MLOps system with streaming ingestion, medallion data architecture, automated training pipelines, and real-time model serving.”

Separation of concerns
Real-time + batch architecture
Production readiness

Before training:
    df_train = df.dropna(subset=["target"])
For real-time prediction:
    👉 Keep the last row:
    df_latest = df.orderBy("open_time", ascending=False).limit(1)