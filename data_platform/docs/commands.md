### setup kafka using
docker compose up -d

### create price topic
docker exec -it crypto-kafka bash

export PATH=$PATH:/opt/kafka/bin

kafka-topics.sh --create --topic crypto_prices --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

kafka-topics.sh --create --topic predictions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

### check topics
kafka-topics.sh --list --bootstrap-server crypto-kafka:9092

docker exec -it crypto-kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092


### streaming pipeline run
docker compose up

ENV=host python3 -m pipelines.ingestion.streaming.jobs.crypto_stream_job

ENV=host python3 -m pipelines.ingestion.streaming.spark.spark_streaming

docker exec -it crypto-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic crypto_prices \
  --from-beginning \
  --bootstrap-server localhost:9092


### Starting data_platform
```
clear
docker compose down
docker compose build
docker compose up -d
docker ps -a
```

docker compose -p data_platform up --build -d

http://localhost:8080 - airflow / airflow


### clear data in delta
sudo rm -rf delta/bronze/market delta/silver/market delta/raw_data/market delta/state/market_batch delta/state/market_stream delta/state/monitoring delta/state/predictions delta/gold/market delta/predictions/log_return_lead1

sudo rm -rf delta/state/predictions delta/predictions

### To reset permissions if needed
sudo chown -R $USER:$USER .


### Streaming job
docker exec -it spark-master \
spark-submit pipelines/ingestion/streaming/spark/spark_streaming.py


### Drift monitoring checks
docker exec -it airflow-scheduler python -m models.monitoring.drift

curl http://localhost:8000/drift


### Run drift monitor manually
docker exec -it airflow-scheduler python -m models.monitoring.drift


### Manual retraining trigger via Airflow API
curl -u airflow:airflow -X POST http://localhost:8080/api/v1/dags/model_training_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -d '{"conf":{"trigger_source":"manual"}}'


### Dashboard setup
pip install -r dashboard/requirements.txt


### Run dashboard
conda activate crypto && streamlit run dashboard/app.py

http://localhost:8501