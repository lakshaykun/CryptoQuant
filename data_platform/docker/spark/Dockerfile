# docker/spark/Dockerfile

FROM apache/spark:3.5.0

USER root

COPY docker/spark/requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV PYTHONPATH=/opt/app

WORKDIR /opt/app