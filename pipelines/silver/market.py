# pipelines/silver/market.py

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark import StorageLevel

from pyspark.sql.window import Window

def transform_bronze_to_silver(df):
    return df.selectExpr(
        "symbol",
        "open_time as timestamp",
        "cast(open as double) as open",
        "cast(high as double) as high",
        "cast(low as double) as low",
        "cast(close as double) as close",
        "cast(volume as double) as volume",
        "cast(trades as int) as trades",
        "cast(taker_buy_base as double) as taker_buy_base",
        "date",
        "ingestion_time",
        "last_processed_time"
    )

def bronze_to_silver(spark, source_path, dest_path, logger):
    if source_path is None or dest_path is None:
        logger.error("Source and destination paths must be provided")
        return

    df = spark.read.format("delta").load(source_path)
    
    # remove all nulls from all columns
    df = df.dropna(subset=[
        "symbol",
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ])

    # Enforce schema
    df = transform_bronze_to_silver(df)

    # Data quality checks
    df = df.filter(
        (F.col("low") <= F.col("open")) &
        (F.col("open") <= F.col("high")) &
        (F.col("low") <= F.col("close")) &
        (F.col("close") <= F.col("high"))
    )

    df = df.filter(
        (F.col("volume") >= 0) &
        (F.col("trades") >= 0) &
        (F.col("taker_buy_base") >= 0)
    )

    # 4. Deduplicate
    window = Window.partitionBy("symbol", "timestamp").orderBy(F.col("ingestion_time").desc())

    df = (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


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
    logger.info(f"Written transformed data to {dest_path}")