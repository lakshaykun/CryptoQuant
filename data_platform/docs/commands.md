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

http://localhost:8080 - airflow / airflow


### clear data in delta
sudo rm -rf delta/bronze/market delta/silver/market delta/raw_data/market delta/state/market delta/gold/market delta/checkpoints

### To reset permissions if needed
sudo chown -R $USER:$USER .


### Streaming job
docker exec -it spark-master \
spark-submit pipelines/ingestion/streaming/spark/spark_streaming.py


### Monitoring stack
docker compose up -d prometheus pushgateway node-exporter cadvisor kafka-exporter airflow-statsd-exporter api

http://localhost:9090  # Prometheus UI
http://localhost:8000/metrics  # FastAPI metrics
http://localhost:9100/metrics  # Node exporter
http://localhost:9091/metrics  # Pushgateway


### Run drift monitor manually
docker exec -it airflow-scheduler python -m models.monitoring.drift


### Manual retraining trigger via Airflow API
curl -u airflow:airflow -X POST http://localhost:8080/api/v1/dags/model_training_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -d '{"conf":{"trigger_source":"manual"}}'


### Dashboard setup
pip install -r dashboard/requirements.txt


### Run dashboard
streamlit run dashboard/app.py

http://localhost:8501