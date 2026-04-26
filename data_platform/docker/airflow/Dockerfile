FROM apache/airflow:2.9.1-python3.10

# Copy requirements first
COPY docker/airflow/requirements.txt /requirements.txt

# Install as airflow user (IMPORTANT)
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

# Switch to root for system deps
USER root
RUN apt-get update && \
    apt-get install -y libgomp1 default-jre-headless && \
    apt-get clean

# Env + workdir
ENV AIRFLOW_HOME="/opt/app/.airflow"
ENV PROJECT_ROOT="/opt/app"

WORKDIR /opt/app

# Copy entrypoint
COPY docker/airflow/entrypoint.sh /opt/app/docker/airflow/entrypoint.sh
RUN chmod +x /opt/app/docker/airflow/entrypoint.sh

# Back to airflow
USER airflow

ENTRYPOINT ["/opt/app/docker/airflow/entrypoint.sh"]