pipelines/
│
├── ingestion/
│   ├── batch/
│   │   ├── sources/
│   │   │   ├── binance.py
│   │   │   ├── api_client.py
│   │   │   └── base.py
│   │   │
│   │   ├── jobs/
│   │   │   ├── backfill_job.py
│   │   │   └── daily_job.py
│   │   │
│   │   └── runner.py
│   │
│   ├── streaming/
│   │   ├── producers/
│   │   │   ├── kafka_producer.py
│   │   │   └── binance_ws.py
│   │   │
│   │   ├── consumers/
│   │   │   ├── spark_consumer.py
│   │   │   └── parser.py
│   │   │
│   │   └── runner.py
│
├── transformations/
│   ├── bronze/
│   │   └── market.py
│   │
│   ├── silver/
│   │   └── market.py
│   │
│   ├── gold/
│       └── market.py
│
├── storage/
│   ├── delta/
│   │   ├── writer.py
│   │   ├── reader.py
│   │   └── utils.py
│   │
│   └── postgres/   # Not decided
│       ├── writer.py
│       └── schema.sql
│
├── orchestration/
│   ├── airflow/
│   │   ├── dags/
│   │   │   ├── batch_pipeline.py
│   │   │   └── streaming_pipeline.py
│   │   └── operators/
│   │       └── spark_submit.py
│   │
│   └── jobs/
│       ├── run_batch.py
│       └── run_streaming.py
│
├── common/
│   ├── config/
│   │   ├── settings.py
│   │   └── constants.py
│   │
│   ├── utils/
│   │   ├── logger.py
│   │   ├── spark.py
│   │   └── time.py
│   │
│   └── schemas/
│       └── market_data.py
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── scripts/
│   ├── start_kafka.sh
│   └── start_spark.sh
│
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── requirements.txt
└── README.md