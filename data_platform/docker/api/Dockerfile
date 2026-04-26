FROM python:3.10-slim

USER root

RUN apt-get update && apt-get install -y \
    libgomp1 \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

COPY docker/api/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/opt/app
WORKDIR /opt/app
