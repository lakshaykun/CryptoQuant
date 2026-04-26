from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F


class GoldSentimentTransformer:
    @staticmethod
    def transform(
        df: SparkDataFrame,
        window_duration: str,
        watermark_duration: str,
    ) -> SparkDataFrame:
        weighted_col = F.col("weighted_sentiment") if "weighted_sentiment" in df.columns else F.lit(0.0)
        confidence_col = F.col("confidence") if "confidence" in df.columns else F.lit(0.0)

        compact = df.select(
            "event_time",
            "symbol",
            weighted_col.alias("weighted_sentiment"),
            confidence_col.alias("confidence"),
        )
        grouped = compact.groupBy(
            F.window(F.col("event_time"), window_duration),
            F.col("symbol"),
        )

        if compact.isStreaming:
            grouped = compact.withWatermark("event_time", watermark_duration).groupBy(
                F.window(F.col("event_time"), window_duration),
                F.col("symbol"),
            )

        return grouped.agg(
            F.avg("weighted_sentiment").alias("sentiment_index"),
            F.avg("confidence").alias("avg_confidence"),
            F.count("*").alias("message_count"),
        )
