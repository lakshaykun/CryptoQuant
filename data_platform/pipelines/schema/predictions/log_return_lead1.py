from pyspark.sql.types import *


PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA = StructType([
    StructField("open_time", TimestampType(), False),
    StructField("symbol", StringType(), False),
    StructField("date", DateType(), True),
    StructField("prediction", DoubleType(), True),
    StructField("ingestion_time", TimestampType(), True)
])