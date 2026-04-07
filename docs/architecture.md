CryptoQuant/
│
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .env
├── .gitignore
│
├── configs/                  # central configs (VERY important)
│   ├── kafka.yaml
│   ├── spark.yaml
│   ├── airflow.yaml
│   └── model.yaml
│
├── datasets/                     # (optional local dev only)
│
├── pipelines/                # core data pipelines
│
├── medallion/               # data lake structure (Delta Lake)
│   ├── bronze/
│   │   ├── market/
│   │   └── articles/
│   ├── silver/
│   └── gold/
│
├── models/                  # ML logic
│
├── notebooks/              # experimentation (optional)
│   ├── eda.ipynb
│   └── experiments.ipynb
│
├── airflow/                # orchestration
│   ├── dags/
│   │   ├── training_dag.py
│   │   ├── retraining_dag.py
│   │   └── drift_dag.py
│   │
│   ├── plugins/
│   └── requirements.txt
│
├── api/                    # model serving
│   ├── app.py              # FastAPI entry
│   ├── routes/
│   │   ├── predict.py
│   │   └── health.py
│   │
│   ├── services/
│   │   ├── inference.py
│   │   └── model_loader.py
│   │
│   └── schemas/
│       └── request.py
│
├── monitoring/             # observability
│   ├── drift.py
│   ├── metrics.py
│   └── alerts.py
│
├── tests/                  # unit + integration tests
│   ├── test_pipeline.py
│   ├── test_model.py
│   └── test_api.py
│
├── scripts/                # utility scripts
│   ├── start_kafka.sh
│   ├── start_spark.sh
│   └── run_pipeline.sh
│
├── ci-cd/                  # CI/CD configs
│   └── github/
│       └── workflows/
│           └── ci.yml
│
└── docs/                   # documentation
    ├── architecture.md
    └── setup.md


## Models
models/
│
├── config/
│   └── model_config.py
│
├── data/
│   ├── loader.py          # read from Silver
│   └── schema.py          # expected columns
│
├── features/
│   ├── build_features.py      # feature engineering logic
│   └── scaling.py             # normalization / scaling
├── training/
│   ├── train.py
│   ├── trainer.py
│   └── hyperparameter_tuning.py
│
├── evaluation/
│   ├── evaluate.py
│   ├── backtesting.py
│   └── metrics.py
│
├── inference/
│   ├── realtime.py        # Kafka/Spark inference
│   └── pipeline.py
│
├── registry/
│   ├── mlflow_registry.py
│   └── model_loader.py
│
└── artifacts/
    ├── models/                # saved models
    └── scalers/



# Pipelines
pipelines/
│
├── ingestion/                      # DATA ENTRY POINTS
│   ├── batch/
│   │   ├── market.py              # batch crypto ingestion pipeline
│   │   ├── sentiment.py           # batch news/reddit ingestion
│   │   ├── fetch_coins.py         # Binance downloader (your code)
│   │   └── utils.py               # date utils, incremental logic
│   │
│   ├── streaming/
│   │   ├── producer.py            # websocket → kafka
│   │   ├── kafka_producer.py      # kafka producer wrapper
│   │   ├── kafka_consumer.py      # kafka consumer (debug/testing)
│   │   ├── spark_streaming.py     # main Spark streaming job
│   │   └── schemas.py             # streaming JSON schema
│
├── bronze/                        # RAW DATA WRITING LAYER
│   ├── market.py                 # write_to_bronze (your code)
│   ├── sentiment.py              # write sentiment data
│   ├── utils.py                  # merge helpers, partitioning
│   └── schema.py                 # MARKET_SCHEMA (important)
│
├── silver/                        # CLEANED + STANDARDIZED DATA
│   ├── market.py                 # cleaning, dedup, casting
│   ├── sentiment.py              # NLP cleaning
│   ├── joins.py                  # merge market + sentiment
│   └── utils.py                  # validation helpers
│
├── gold/                          # FEATURE ENGINEERING
│   ├── market_features.py        # returns, volatility, indicators
│   ├── sentiment_features.py     # sentiment scores aggregation
│   ├── feature_store.py          # final ML-ready dataset
│   └── utils.py
│
├── orchestration/                 # PIPELINE EXECUTION LOGIC
│   ├── batch_pipeline.py         # bronze → silver → gold (batch)
│   ├── streaming_pipeline.py     # streaming end-to-end
│   └── scheduler.py              # cron / airflow hooks
│
├── validation/                    # DATA QUALITY (VERY IMPORTANT)
│   ├── market.py                 # schema + null checks
│   ├── sentiment.py
│   └── expectations.py           # reusable rules
│
├── state/                         # INCREMENTAL STATE MANAGEMENT
│   ├── market_state.py           # last timestamp logic
│   └── state_store.py            # file/db abstraction
│
└── utils/                         # SHARED UTILITIES
    ├── logger.py
    ├── config_loader.py          # load YAML configs
    ├── spark.py                  # Spark session builder
    └── helpers.py