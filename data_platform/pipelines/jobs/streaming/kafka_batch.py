# pipelines/jobs/streaming/kafka_batch.py


import json

def get_kafka_batch(spark, topic, brokers, offsets):

    # Partition discovery (legal config)
    partitions_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select("partition")
        .distinct()
    )

    partitions = [row["partition"] for row in partitions_df.collect()]

    reader = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("failOnDataLoss", "false")
    )

    if offsets:
        fixed_offsets = {topic: {}}

        for p in partitions:
            if str(p) in offsets.get(topic, {}):
                fixed_offsets[topic][str(p)] = offsets[topic][str(p)]
            else:
                fixed_offsets[topic][str(p)] = -2  # earliest

        reader = reader.option("startingOffsets", json.dumps(fixed_offsets))

    else:
        reader = reader.option("startingOffsets", "earliest")

    reader = reader.option("endingOffsets", "latest")

    return reader.load(), topic