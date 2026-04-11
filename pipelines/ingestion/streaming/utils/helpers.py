# pipelines/ingestion/streaming/utils/helpers.py

from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col

def parse_kafka_message(
    df: DataFrame, 
    schema: StructType
) -> DataFrame:
    return df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")
