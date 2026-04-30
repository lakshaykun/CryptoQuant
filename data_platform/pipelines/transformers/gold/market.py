# pipelines/transformations/gold/market.py

from datetime import timedelta

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.window import Window
from pipelines.storage.delta.reader import read_incremental

class GoldMarketTransformer:

    @staticmethod
    def _safe_divide(numerator, denominator, eps=1e-6):
        return F.when(
            denominator.isNull() | (F.abs(denominator) < eps),
            F.lit(0.0)
        ).otherwise(numerator / denominator)

    @staticmethod
    def transform(df: SparkDataFrame) -> SparkDataFrame:

        if df is None:
            return None

        if not df.isStreaming:
            if not df.head(1):
                return df.limit(0)

        df = df.withColumn("open_time_sec", F.col("open_time").cast("long"))
        base_window = Window.partitionBy("symbol").orderBy("open_time_sec")
        window_5 = base_window.rangeBetween(-1200, 0)
        window_20 = base_window.rangeBetween(-5700, 0)
        window_1d = base_window.rangeBetween(-86400, 0)
        window_3d = base_window.rangeBetween(-259200, 0)

        # =========================
        # BASE FEATURES
        # =========================

        df = df.withColumn("hl_range", (F.col("high") - F.col("low")) / F.col("close"))

        df = df.withColumn(
            "vwap_proxy",
            (F.col("close") / ((F.col("high") + F.col("low") + F.col("close")) / 3)) - 1
        )

        prev_close = F.lag("close", 1).over(base_window)

        df = df.withColumn(
            "log_return_raw",
            F.when(
                (F.col("close") > 0) & (prev_close > 0),
                F.log(F.col("close") / prev_close)
            ).otherwise(F.lit(0.0))
        )

        # =========================
        # RETURNS
        # =========================

        df = df.withColumn(
            "log_return_lag1",
            F.lag("log_return_raw", 1).over(base_window)
        )

        df = df.withColumn(
            "return_5",
            F.sum("log_return_raw").over(window_5)
        )

        df = df.withColumn(
            "return_acceleration",
            F.col("log_return_raw") - F.lag("log_return_raw", 1).over(base_window)
        )

        mean_20 = F.avg("log_return_raw").over(window_20)
        std_20 = F.stddev("log_return_raw").over(window_20)

        df = df.withColumn(
            "return_zscore",
            GoldMarketTransformer._safe_divide(
                F.col("log_return_raw") - mean_20,
                std_20
            )
        )

        df = df.withColumn(
            "smoothed_return_3",
            F.avg("log_return_raw").over(base_window.rangeBetween(-600, 0))
        )

        df = df.withColumn("return_1d", F.sum("log_return_raw").over(window_1d))
        df = df.withColumn("return_3d", F.sum("log_return_raw").over(window_3d))

        # =========================
        # VOLATILITY
        # =========================

        df = df.withColumn(
            "volatility",
            GoldMarketTransformer._safe_divide(
                F.col("high") - F.col("low"),
                F.col("close")
            )
        )

        df = df.withColumn(
            "volatility_5",
            F.avg("volatility").over(window_5)
        )

        df = df.withColumn(
            "volatility_std_10",
            F.stddev("log_return_raw").over(base_window.rangeBetween(-2700, 0))
        )

        df = df.withColumn("volatility_1d", F.stddev("log_return_raw").over(window_1d))
        df = df.withColumn("volatility_3d", F.stddev("log_return_raw").over(window_3d))

        df = df.withColumn(
            "volatility_ratio",
            GoldMarketTransformer._safe_divide(
                F.col("volatility"),
                F.col("volatility_5")
            )
        )

        df = df.withColumn(
            "volatility_regime",
            GoldMarketTransformer._safe_divide(
                F.col("volatility_5"),
                F.avg("volatility_5").over(window_20)
            )
        )

        # =========================
        # MICROSTRUCTURE
        # =========================

        df = df.withColumn(
            "imbalance_ratio",
            GoldMarketTransformer._safe_divide(
                F.col("taker_buy_base") - (F.col("volume") - F.col("taker_buy_base")),
                F.col("volume")
            )
        )

        df = df.withColumn(
            "buy_ratio",
            GoldMarketTransformer._safe_divide(
                F.col("taker_buy_base"),
                F.col("volume")
            )
        )

        df = df.withColumn(
            "imbalance_change",
            F.col("imbalance_ratio") - F.lag("imbalance_ratio", 1).over(base_window)
        )

        df = df.withColumn(
            "imbalance_momentum",
            F.col("imbalance_ratio") - F.lag("imbalance_ratio", 3).over(base_window)
        )

        df = df.withColumn(
            "buy_pressure_change",
            F.col("buy_ratio") - F.avg("buy_ratio").over(window_5)
        )

        # =========================
        # PRICE POSITIONING
        # =========================

        df = df.withColumn("ma_5_tmp", F.avg("close").over(window_5))
        df = df.withColumn("ma_20_tmp", F.avg("close").over(window_20))

        df = df.withColumn(
            "price_to_ma_5",
            GoldMarketTransformer._safe_divide(
                F.col("close"),
                F.col("ma_5_tmp")
            ) - 1
        )

        df = df.withColumn(
            "price_to_ma_20",
            GoldMarketTransformer._safe_divide(
                F.col("close"),
                F.col("ma_20_tmp")
            ) - 1
        )

        df = df.withColumn(
            "ma_cross_5_20",
            GoldMarketTransformer._safe_divide(
                F.col("ma_5_tmp"),
                F.col("ma_20_tmp")
            ) - 1
        )

        # =========================
        # MOMENTUM
        # =========================

        df = df.withColumn(
            "momentum_ratio",
            GoldMarketTransformer._safe_divide(
                F.col("close"),
                F.col("ma_5_tmp")
            )
        )

        # =========================
        # VOLUME & ACTIVITY
        # =========================

        df = df.withColumn("volume_avg_20", F.avg("volume").over(window_20))

        df = df.withColumn(
            "volume_spike",
            GoldMarketTransformer._safe_divide(
                F.col("volume"),
                F.col("volume_avg_20")
            )
        )

        df = df.withColumn(
            "volume_ratio",
            GoldMarketTransformer._safe_divide(
                F.col("volume"),
                F.col("volume_avg_20")
            )
        )

        df = df.withColumn(
            "volume_trend",
            GoldMarketTransformer._safe_divide(
                F.avg("volume").over(window_5),
                F.col("volume_avg_20")
            )
        )

        df = df.withColumn(
            "trades_ratio",
            GoldMarketTransformer._safe_divide(
                F.col("trades"),
                F.avg("trades").over(window_20)
            )
        )

        # =========================
        # CANDLE STRUCTURE
        # =========================

        df = df.withColumn(
            "body_size",
            GoldMarketTransformer._safe_divide(
                F.col("close") - F.col("open"),
                F.col("close")
            )
        )

        df = df.withColumn(
            "close_position",
            GoldMarketTransformer._safe_divide(
                F.col("close") - F.col("low"),
                F.col("high") - F.col("low")
            )
        )

        df = df.withColumn(
            "range_ratio",
            GoldMarketTransformer._safe_divide(
                F.col("hl_range"),
                F.avg("hl_range").over(window_20)
            )
        )

        # =========================
        # TIME FEATURES
        # =========================

        df = df.withColumn("hour", F.hour("open_time"))
        df = df.withColumn("hour_sin", F.sin(2 * 3.1415926535 * F.col("hour") / 24))
        df = df.withColumn("hour_cos", F.cos(2 * 3.1415926535 * F.col("hour") / 24))
        df = df.drop("hour")

        # =========================
        # METADATA
        # =========================

        df = df.withColumn(
            "is_valid_feature_row",
            F.col("log_return_lag1").isNotNull() &
            F.col("return_5").isNotNull() &
            F.col("range_ratio").isNotNull()
        )

        df = df.withColumn("date", F.to_date("open_time"))

        # =========================
        # CLEANUP
        # =========================

        df = df.replace([float("inf"), float("-inf")], 0)
        df = df.fillna(0)

        df = df.withColumnRenamed("log_return_raw", "return_current")

        # backward compatibility
        df = df.withColumn("log_return", F.col("return_current"))
        df = df.withColumn(
            "log_return_lag2",
            F.lag("log_return", 2).over(base_window)
        )
        df = df.withColumn(
            "buy_ratio_lag1",
            F.lag("buy_ratio", 1).over(base_window)
        )
        df = df.withColumn("ma_5", F.col("ma_5_tmp"))
        df = df.withColumn("ma_20", F.col("ma_20_tmp"))
        df = df.withColumn(
            "volume_5",
            F.avg("volume").over(window_5)
        )
        df = df.withColumn(
            "buy_ratio_5",
            F.avg("buy_ratio").over(window_5)
        )
        df = df.withColumn(
            "momentum",
            F.col("close") - F.lag("close", 5).over(base_window)
        )
        df = df.withColumn(
            "price_range_ratio",
            GoldMarketTransformer._safe_divide(
                F.col("high") - F.col("low"),
                F.col("open")
            )
        )
        df = df.withColumn("hour", F.hour("open_time"))
        df = df.withColumn(
            "day_of_week",
            F.dayofweek("open_time")
        )
        df = df.withColumn(
            "trend_strength",
            GoldMarketTransformer._safe_divide(
                F.sum("log_return").over(window_5),
                F.sum(F.abs("log_return")).over(window_5)
            )
        )
                
        # drop intermediate columns
        df = df.drop(
            "ma_5_tmp",
            "ma_20_tmp",
            "volume_avg_20",
            "open_time_sec"
        )

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