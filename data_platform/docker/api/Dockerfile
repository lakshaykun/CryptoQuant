FROM python:3.10-slim

USER root

COPY docker/api/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/opt/app
WORKDIR /opt/app
