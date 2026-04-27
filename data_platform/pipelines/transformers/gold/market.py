# pipelines/transformations/gold/market.py

from datetime import timedelta

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.window import Window
from pipelines.storage.delta.reader import read_incremental

class GoldMarketTransformer:
    @staticmethod
    def transform(df: SparkDataFrame) -> SparkDataFrame:
        '''Feature engineering for market data. Creates new features based on existing columns of silver market data.'''
        
        if df is None:
            return None

        if not df.isStreaming:
            if not df.head(1):
                return df.limit(0)
        
        # log_return = log(close / open)
        df = df.withColumn(
            "log_return",
            F.when(F.col("open") > 0, F.log(F.col("close") / F.col("open")))
            .otherwise(0.0)
        )

        # volatility = (high - low) / close
        df = df.withColumn(
            "volatility",
            F.when(F.col("close") > 0,
                (F.col("high") - F.col("low")) / F.col("close"))
            .otherwise(0.0)
        )

        # imbalance = taker_buy_base - (volume - taker_buy_base)
        df = df.withColumn(
            "imbalance_ratio",
            F.when(F.col("volume") > 0,
                (F.col("taker_buy_base") - (F.col("volume") - F.col("taker_buy_base"))) / F.col("volume"))
            .otherwise(0.0)
        )

        # buy_ratio = taker_buy_base / volume
        df = df.withColumn(
            "buy_ratio",
            F.when(F.col("volume") > 0,
                F.col("taker_buy_base") / F.col("volume"))
            .otherwise(0.0)
        )

        base_window = Window.partitionBy("symbol").orderBy("open_time")


        df = df.withColumn("log_return_lag1", F.lag("log_return", 1).over(base_window))
        df = df.withColumn("log_return_lag2", F.lag("log_return", 2).over(base_window))
        df = df.withColumn("buy_ratio_lag1", F.lag("buy_ratio", 1).over(base_window))

        window_5 = base_window.rowsBetween(-5, -1)
        window_20 = base_window.rowsBetween(-20, -1)

        df = df.withColumn("ma_5", F.avg("close").over(window_5))
        df = df.withColumn("ma_20", F.avg("close").over(window_20))

        df = df.withColumn("volatility_5", F.avg("volatility").over(window_5))
        df = df.withColumn("volume_5", F.avg("volume").over(window_5))
        df = df.withColumn("buy_ratio_5", F.avg("buy_ratio").over(window_5))

        df = df.withColumn(
            "momentum_ratio",
            F.when(F.col("ma_5") > 0,
                F.col("close") / F.col("ma_5"))
            .otherwise(0.0)
        )
        df = df.withColumn(
            "volume_spike",
            F.when(F.col("volume_5") > 0,
                F.col("volume") / F.col("volume_5"))
            .otherwise(0.0)
        )

        df = df.withColumn(
            "body_size",
            F.when(F.col("close") > 0,
                (F.col("close") - F.col("open")) / F.col("close"))
            .otherwise(0.0)
        )
        df = df.withColumn("trend_strength", F.col("ma_5") - F.col("ma_20"))

        df = df.withColumn(
            "volatility_ratio",
            F.when(F.col("volatility_5") > 0,
                F.col("volatility") / F.col("volatility_5"))
            .otherwise(0.0)
        )
        
        df = df.withColumn("return_5", F.sum("log_return").over(window_5))
        df = df.withColumn("return_20", F.sum("log_return").over(window_20))
        df = df.withColumn(
            "volatility_std_10",
            F.coalesce(
                F.stddev("log_return").over(base_window.rowsBetween(-10, -1)),
                F.lit(0.0)
            )
        )

        window_50 = base_window.rowsBetween(-50, -1)

        df = df.withColumn("ma_50", F.avg("close").over(window_50))
        df = df.withColumn("trend_long", F.col("ma_20") - F.col("ma_50"))

        df = df.withColumn(
            "imbalance_change",
            F.col("imbalance_ratio") - F.lag("imbalance_ratio", 1).over(base_window)
        )

        df = df.withColumn("volatility_momentum", F.col("volatility") * F.col("momentum_ratio"))
        
        df = df.withColumn(
            "trend_regime",
            F.when(F.abs(F.col("trend_long")) > F.stddev("trend_long").over(window_20), 1).otherwise(0)
        )

        df = df.withColumn(
            "price_deviation",
            (F.col("close") - F.col("ma_20")) / F.col("ma_20")
        )

        df = df.withColumn("hour", F.hour(F.col("open_time")))
        df = df.withColumn("hour_sin", F.sin(2 * 3.1415 * F.col("hour") / 24))
        df = df.withColumn("hour_cos", F.cos(2 * 3.1415 * F.col("hour") / 24))
        df = df.drop("hour")


        # metadata
        df = df.withColumn(
            "is_valid_feature_row",
            (
                F.col("log_return_lag2").isNotNull() &
                F.col("log_return_lag1").isNotNull() &
                F.col("ma_20").isNotNull() &
                F.col("ma_5").isNotNull() &
                F.col("buy_ratio_lag1").isNotNull() &
                F.col("volatility_5").isNotNull() &
                F.col("volume_5").isNotNull() &
                F.col("buy_ratio_5").isNotNull()
            )
        )

        df = df.withColumn("date", F.to_date(F.col("open_time")))

        return df
    

    @staticmethod
    def process_gold_stream_batch(
        df: SparkDataFrame
    ):
        """
        For stream pipelines, we need to load historical silver data to calculate features that require past values (e.g. moving averages, lags). This function handles that logic.
        """
        spark = df.sparkSession
        # get minimum open_time in the batch
        min_open_time = df.select(F.min("open_time")).first()[0]

        min_fetch_time = min_open_time - timedelta(minutes=21)  # 20 minutes for ma_20 + 1 minute buffer

        # Load historical silver
        historical_df = read_incremental(
            spark, 
            "silver_market",  
            min_fetch_time
        )

        historical_df = historical_df.drop("ingestion_time")

        historical_df = historical_df.join(
            df.select("symbol").distinct(),
            on="symbol",
            how="inner"
        )

        combined = (
            historical_df.unionByName(df)
            .dropDuplicates(["symbol", "open_time"])
        )

        # Apply transformations
        combined = GoldMarketTransformer.transform(combined)

        df_keys = df.select("symbol", "open_time").dropDuplicates()
        result = combined.join(
            df_keys,
            on=["symbol", "open_time"],
            how="inner"
        )

        return result