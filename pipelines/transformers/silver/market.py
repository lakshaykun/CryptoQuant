# pipelines/transformations/silver/market.py

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame


class SilverMarketTransformer:
    @staticmethod
    def transform(df: SparkDataFrame) -> SparkDataFrame:
        """
        Cleans and standardizes bronze → silver.
        Removes invalid data and enforces types.
        """

        if df is None:
            return None

        if not df.isStreaming:
            if not df.head(1):
                return df.limit(0)

        # ---------------------------
        # Drop unnecessary columns
        # ---------------------------
        df = df.select(
            "symbol",
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trades",
            "taker_buy_base",
            "date",
        )

        # ---------------------------
        # Drop nulls (critical fields)
        # ---------------------------
        df = df.dropna(subset=[
            "symbol",
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ])

        # ---------------------------
        # Cast types (standardization)
        # ---------------------------
        df = df.selectExpr(
            "symbol",
            "open_time",
            "cast(open as double) as open",
            "cast(high as double) as high",
            "cast(low as double) as low",
            "cast(close as double) as close",
            "cast(volume as double) as volume",
            "cast(trades as int) as trades",
            "cast(taker_buy_base as double) as taker_buy_base",
            "date"
        )

        # ---------------------------
        # Enforce business rules
        # ---------------------------
        df = df.filter(
            (F.col("low") <= F.col("open")) &
            (F.col("open") <= F.col("high")) &
            (F.col("low") <= F.col("close")) &
            (F.col("close") <= F.col("high")) &
            (F.col("volume") >= 0) &
            (F.col("trades") >= 0) &
            (F.col("taker_buy_base") >= 0)
        )

        return df