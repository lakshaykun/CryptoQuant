FROM apache/airflow:2.9.1-python3.10

USER airflow

RUN pip install --no-cache-dir psycopg2-binary

ENV AIRFLOW_HOME="/opt/app/.airflow"
ENV PROJECT_ROOT="/opt/app"

WORKDIR /opt/app

COPY docker/airflow/entrypoint.sh /opt/app/docker/airflow/entrypoint.sh

USER root
RUN chmod +x /opt/app/docker/airflow/entrypoint.sh

USER airflow

ENTRYPOINT ["/opt/app/docker/airflow/entrypoint.sh"]