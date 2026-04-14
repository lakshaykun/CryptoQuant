FROM apache/spark:3.5.0

USER root

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV PYTHONPATH=/opt/app

WORKDIR /opt/app