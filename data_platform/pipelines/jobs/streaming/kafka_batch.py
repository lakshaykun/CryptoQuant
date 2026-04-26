# pipelines/jobs/streaming/kafka_batch.py

import json

def get_kafka_batch(spark, topic, brokers, offsets):
    reader = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
    )

    if offsets:
        reader = reader.option(
            "startingOffsets",
            json.dumps(offsets)
        )
    else:
        reader = reader.option("startingOffsets", "earliest")

    reader = reader.option("endingOffsets", "latest")

    return reader.load(), topic