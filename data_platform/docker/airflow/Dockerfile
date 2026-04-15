FROM apache/airflow:2.9.1-python3.10

USER airflow

COPY docker/airflow/requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV AIRFLOW_HOME="/opt/app/.airflow"
ENV PROJECT_ROOT="/opt/app"

WORKDIR /opt/app

COPY docker/airflow/entrypoint.sh /opt/app/docker/airflow/entrypoint.sh

USER root
RUN chmod +x /opt/app/docker/airflow/entrypoint.sh

USER airflow

ENTRYPOINT ["/opt/app/docker/airflow/entrypoint.sh"]