from pyspark.sql.types import *


PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA = StructType([
    StructField("open_time", TimestampType(), False),
    StructField("symbol", StringType(), False),
    StructField("date", DateType(), True),
    StructField("return_short", DoubleType(), True),
    StructField("return_long", DoubleType(), True),
    StructField("sign_short", IntegerType(), True),
    StructField("sign_long", IntegerType(), True),
    StructField("prediction", DoubleType(), True),
    StructField("ingestion_time", TimestampType(), True)
])