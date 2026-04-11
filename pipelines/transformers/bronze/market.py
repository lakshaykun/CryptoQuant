# pipelines/transformations/bronze/market.py

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pandas import DataFrame as PandasDataFrame
from typing import Optional, Union


from pipelines.schema.raw.market import RAW_MARKET_SCHEMA

class BronzeMarketTransformer:
    @staticmethod
    def transform(
        df: Union[SparkDataFrame, PandasDataFrame], 
        source: str = "stream", 
        spark: Optional[SparkSession] = None
    ) -> SparkDataFrame:
        '''Transforms raw market data into a standardized format for bronze layer.'''
        
        if df is None:
            return None
        
        # If the input is a Pandas DataFrame, convert it to a Spark DataFrame with the appropriate schema
        if isinstance(df, PandasDataFrame):
            if spark is None:
                raise ValueError("SparkSession is required for Pandas -> Spark conversion")
            
            df = df[[
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
            df = spark.createDataFrame(df, schema=RAW_MARKET_SCHEMA)
        
        if not df.isStreaming:
            if not df.head(1):
                return df.limit(0)
        
        df.select("open_time").show(5, False)

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
        return F.from_unixtime((col / 1000000).cast("long")).cast("timestamp")