FROM python:3.11-slim

WORKDIR /app

COPY delta_sync/sentiment/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_platform/configs/ data_platform/configs/
COPY data_platform/pipelines/ data_platform/pipelines/
COPY data_platform/utils_global/ data_platform/utils_global/
COPY delta_sync/sentiment/ delta_sync/sentiment/

CMD ["python", "-m", "delta_sync.sentiment.scheduler"]