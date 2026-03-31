### setup kafka using
docker compose up -d

### create price topic
docker exec -it crypto-kafka bash

export PATH=$PATH:/opt/kafka/bin

kafka-topics.sh --create --topic crypto_prices --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

kafka-topics.sh --create --topic predictions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

### check topics
kafka-topics.sh --list --bootstrap-server localhost:9092