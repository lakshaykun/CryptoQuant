FROM apache/kafka:3.7.0

WORKDIR /opt/bootstrap

COPY infra/kafka/create_topics.sh /opt/bootstrap/create_topics.sh

RUN chmod +x /opt/bootstrap/create_topics.sh

CMD ["/bin/bash", "-lc", "echo Kafka admin image ready; sleep infinity"]
