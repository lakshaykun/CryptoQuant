# pipelines/bronze/market.py

import pandas
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pipelines.utils.schema import RAW_MARKET_SCHEMA


def write_to_bronze(pdf: pandas.DataFrame, spark, path, logger, source = "stream"):
    if pdf is None:
        logger.warning("Empty DataFrame. Skipping write.")
        return
    
    pdf = pdf[[
        "symbol",
        "open_time",
        "close_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote"
    ]]

    # Enforce schema
    df = spark.createDataFrame(pdf, schema=RAW_MARKET_SCHEMA)

    # Standardize timestamps (important!)
    df = (
        df
        .withColumn("open_time", F.col("open_time").cast("timestamp"))
        .withColumn("close_time", F.col("close_time").cast("timestamp"))
    )

    # Add metadata
    df = (
        df
        .withColumn("date", F.to_date("open_time"))
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("source", F.lit(source))  # batch or stream
        .withColumn("last_processed_time", F.lit(None).cast("timestamp"))
    )

    # Drop Duplicates based on natural keys (symbol + open_time)
    df = df.dropDuplicates(["symbol", "open_time"])

    # Write to Delta Lake with upsert logic
    if DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)

        (
            delta_table.alias("t")
            .merge(
                df.alias("s"),
                "t.symbol = s.symbol AND t.open_time = s.open_time"
            )
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
            .save(path)
        )

    logger.info("Bronze write successful")