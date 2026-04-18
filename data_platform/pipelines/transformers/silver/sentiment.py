from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F


class SilverSentimentTransformer:
    @staticmethod
    def transform(df: SparkDataFrame) -> SparkDataFrame:
        if df is None:
            return None

        if not df.isStreaming:
            if not df.head(1):
                return df.limit(0)

        cleaned = (
            df
            .filter(F.col("text").isNotNull())
            .withColumn("text", F.regexp_replace("text", r"<[^>]+>|&amp;|&lt;|&gt;|&(quot|#34);|&#39;", " "))
            .withColumn("text", F.lower(F.col("text")))
            .withColumn("text", F.regexp_replace("text", r"https?://\S+|www\.\S+|@(\w+)", " "))
            .withColumn("text", F.regexp_replace("text", r"#(\w+)", "$1"))
            .withColumn("text", F.regexp_replace("text", r"[^a-z0-9$%+\-\s]", " "))
            .withColumn("text", F.regexp_replace("text", r"\s+", " "))
            .withColumn("text", F.trim(F.col("text")))
            .withColumn("source_norm", F.lower(F.trim(F.col("source"))))
            .withColumn(
                "text",
                F.when(
                    F.col("source_norm") == "reddit",
                    F.regexp_replace(
                        F.regexp_replace(F.col("text"), r"\b([ru])/\w+\b", " "),
                        r"\b(removed|deleted)\b",
                        " ",
                    ),
                ).otherwise(F.col("text")),
            )
            .withColumn(
                "text",
                F.when(
                    F.col("source_norm") == "youtube",
                    F.regexp_replace(
                        F.regexp_replace(F.col("text"), r"\b\d{1,2}:\d{2}(?::\d{2})?\b", " "),
                        r"\[[^\]]+\]",
                        " ",
                    ),
                ).otherwise(F.col("text")),
            )
            .withColumn(
                "text",
                F.when(
                    F.col("source_norm") == "news",
                    F.regexp_replace(
                        F.regexp_replace(F.col("text"), r"\b(source|via)\s*:\s*\S+", " "),
                        r"\b(read more|continue reading)\b.*$",
                        " ",
                    ),
                ).otherwise(F.col("text")),
            )
            .withColumn("text", F.regexp_replace("text", r"\s+", " "))
            .withColumn("text", F.trim(F.col("text")))
            .withColumn(
                "event_time",
                F.coalesce(
                    F.to_timestamp(F.col("timestamp")),
                    F.to_timestamp(F.from_unixtime(F.col("timestamp").cast("double"))),
                    F.to_timestamp(F.from_unixtime((F.col("timestamp").cast("double") / 1000.0))),
                ),
            )
            .drop("source_norm")
        )

        return (
            cleaned
            .filter(F.col("event_time").isNotNull())
            .filter(F.length(F.col("text")) > 5)
            .filter(~F.col("text").rlike("(?i)100x|free money|pump signal|join now|whatsapp group|telegram signal"))
        )
