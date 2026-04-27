FROM python:3.11-slim

WORKDIR /app

COPY delta_sync/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_platform/configs/ data_platform/configs/
COPY data_platform/pipelines/ data_platform/pipelines/
COPY data_platform/utils_global/ data_platform/utils_global/
COPY delta_sync/ delta_sync/

CMD ["python", "-m", "delta_sync.scheduler"]