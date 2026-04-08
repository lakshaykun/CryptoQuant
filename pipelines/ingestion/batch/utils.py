# pipelines/ingestion/batch/utils.py

from pyspark.sql import functions as F

def get_last_ingested_timestamp(spark, path, logger, symbols, start_date):
    try:
        df = spark.read.format("delta").load(path)

        # Filter only required symbols (important for performance)
        df = df.filter(F.col("symbol").isin(symbols))

        # Get max timestamp per symbol
        result_df = (
            df.groupBy("symbol")
              .agg(F.max("open_time").alias("max_time"))
        )

        # Convert to dict: symbol -> timestamp
        result = {
            row["symbol"]: row["max_time"]
            for row in result_df.collect()
            if row["max_time"] is not None
        }

        # Ensure all symbols exist in output (even if no data)
        for symbol in symbols:
            if symbol not in result:
                result[symbol] = start_date

        return result

    except Exception as e:
        logger.warning(f"No existing bronze data found: {e}")
        return {symbol: start_date for symbol in symbols}

