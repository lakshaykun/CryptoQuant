# pipelines/ingestion/batch/utils.py

from pyspark.sql import functions as F
from datetime import timedelta

def get_last_ingested_timestamp(spark, path, logger):
    try:
        df = spark.read.format("delta").load(path)

        result = df.select(F.max("open_time").alias("max_time")).collect()[0]

        if result["max_time"] is None:
            return None

        return result["max_time"]

    except Exception as e:
        logger.warning(f"No existing bronze data found: {e}")
        return None
    
def interval_to_timedelta(interval: str):
    if interval.endswith("m"):
        return timedelta(minutes=int(interval[:-1]))
    elif interval.endswith("h"):
        return timedelta(hours=int(interval[:-1]))
    elif interval.endswith("d"):
        return timedelta(days=int(interval[:-1]))
    else:
        raise ValueError(f"Unsupported interval: {interval}")