# pipelines/gold/market.py

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark import StorageLevel

def feature_engineering(df):
    # Create new feature 

    # log_return = log(close / open)
    df = df.withColumn(
        "log_return",
        F.when(F.col("open") > 0, F.log(F.col("close") / F.col("open")))
        .otherwise(0.0)
    )
    # volatility = high - low 
    df = df.withColumn("volatility", F.col("high") - F.col("low"))
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
    from pyspark.sql.window import Window

    base_window = Window.partitionBy("symbol").orderBy("timestamp")

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

    df = df.withColumn("momentum", F.col("close") - F.col("ma_5"))
    df = df.withColumn(
        "volume_spike",
        F.when(F.col("volume_5") > 0,
            F.col("volume") / F.col("volume_5"))
        .otherwise(0.0)
    )
    df = df.withColumn(
        "price_range_ratio",
        F.when(F.col("close") > 0,
            (F.col("high") - F.col("low")) / F.col("close"))
        .otherwise(0.0)
    )

    df = df.withColumn(
        "body_size",
        F.when(F.col("close") > 0,
            (F.col("close") - F.col("open")) / F.col("close"))
        .otherwise(0.0)
    )
    df = df.withColumn("hour", F.hour(F.col("timestamp")))
    df = df.withColumn("day_of_week", F.dayofweek(F.col("timestamp")))
    df = df.withColumn("trend_strength", F.col("ma_5") - F.col("ma_20"))

    df = df.withColumn(
        "volatility_ratio",
        F.when(F.col("volatility_5") > 0,
            F.col("volatility") / F.col("volatility_5"))
        .otherwise(0.0)
    )
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
    return df

def silver_to_gold(spark, source_path, dest_path, logger):
    if source_path is None or dest_path is None:
        logger.error("Source and destination paths must be provided")
        return

    df = spark.read.format("delta").load(source_path)

    # Apply transformations and filters as needed
    df = feature_engineering(df)
    
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    # Write to Delta Lake
    if DeltaTable.isDeltaTable(spark, dest_path):
        delta_table = DeltaTable.forPath(spark, dest_path)

        (
            delta_table.alias("t")
            .merge(
                df.alias("s"),
                "t.symbol = s.symbol AND t.timestamp = s.timestamp"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df
            .write
            .format("delta")
            .partitionBy("symbol", "date")
            .mode("append")
            .save(dest_path)
        )

    df.unpersist()
    logger.info(f"Written processed data to {dest_path}")