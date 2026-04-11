# pipelines/transformations/raw/market.py

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

class RawMarketTransformer:
    @staticmethod
    def transform(df: SparkDataFrame, source: str = "stream") -> SparkDataFrame:
        '''Transforms raw market data into a standardized format for bronze layer.'''
        
        if df is None:
            return None
        
        if df.limit(1).count() == 0:
            return df.limit(0)  # Return empty DataFrame with same schema
        
        # Remove the ignore column
        if "ignore" in df.columns:
            df = df.drop("ignore")       
        
        # Drop Duplicates based on natural keys (symbol + open_time)
        df = df.dropDuplicates(["symbol", "open_time"])

        return df