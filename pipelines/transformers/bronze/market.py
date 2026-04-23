# pipelines/transformations/bronze/market.py

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pandas import DataFrame as PandasDataFrame
from typing import Optional, Union


from pipelines.schema.raw.market import RAW_MARKET_SCHEMA

class BronzeMarketTransformer:
    @staticmethod
    def transform(
        df: SparkDataFrame, 
        source: str = "stream",
    ) -> SparkDataFrame:
        '''Transforms raw market data into a standardized format for bronze layer.'''
        
        if df is None:
            return None
        
        if not df.isStreaming:
            if not df.head(1):
                return df.limit(0)
        
        # Standardize timestamps
        df = df.filter(F.col("open_time") > 1e12)
        
        df = df.withColumn("open_time", BronzeMarketTransformer._normalize_ts(F.col("open_time"))) \
               .withColumn("close_time", BronzeMarketTransformer._normalize_ts(F.col("close_time")))

        # Add metadata
        df = df.withColumn("date", F.to_date("open_time"))  # for partitioning
        df = df.withColumn("source", F.lit(source))  # batch or stream
        
        # Drop Duplicates based on natural keys (symbol + open_time)
        if df.isStreaming:
            df = (
                df
                .withWatermark("open_time", "1 day")
                .dropDuplicates(["symbol", "open_time"])
            )
        else:
            df = df.dropDuplicates(["symbol", "open_time"])

        return df
    
    @staticmethod
    def _normalize_ts(col):
        col = col.cast("long")

        return (
            F.when(col > 1e15, F.from_unixtime((col / 1e6).cast("long")))  # microseconds
            .when(col > 1e12, F.from_unixtime((col / 1e3).cast("long")))  # milliseconds
            .otherwise(F.from_unixtime(col))                              # seconds
            .cast("timestamp")
        )