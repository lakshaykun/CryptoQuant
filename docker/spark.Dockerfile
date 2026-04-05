FROM bitnami/spark:3.5

USER root
COPY . /app
WORKDIR /app

RUN pip install pyspark delta-spark requests

CMD ["bash"]