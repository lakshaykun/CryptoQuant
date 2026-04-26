# docker/stream_producer/Dockerfile

FROM python:3.10-slim

USER root

# Install necessary packages
COPY docker/stream-producer/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


ENV PYTHONPATH=/opt/app
WORKDIR /opt/app