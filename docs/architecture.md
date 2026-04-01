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
│   ├── streaming/
│   │   ├── producer.py      # fetch crypto data (5 min)
│   │   ├── kafka_producer.py
│   │   ├── kafka_consumer.py
│   │   └── spark_streaming.py   # main streaming job
│   │
│   ├── batch/
│   │   ├── feature_engineering.py
│   │   ├── data_validation.py
│   │   └── aggregation.py
│   │
│   └── utils/
│       ├── logger.py
│       ├── helpers.py
│       └── schema.py
│
├── medallion/               # data lake structure (Delta Lake)
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── models/                  # ML logic
│   ├── train.py
│   ├── predict.py
│   ├── evaluate.py
│   ├── features.py
│   └── registry.py         # MLflow integration
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