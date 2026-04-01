import json

def parse_kafka_message(df):
    return df.selectExpr("CAST(value AS STRING) as json") \
             .selectExpr("from_json(json, 'timestamp LONG, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, trades INT, taker_buy_base DOUBLE, symbol STRING') as data") \
             .select("data.*")