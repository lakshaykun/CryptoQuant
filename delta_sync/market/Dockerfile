FROM python:3.11-slim

WORKDIR /app

COPY delta_sync/market/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_platform/configs/ data_platform/configs/
COPY data_platform/pipelines/ data_platform/pipelines/
COPY data_platform/utils_global/ data_platform/utils_global/
COPY delta_sync/market/ delta_sync/market/

CMD ["python", "-m", "delta_sync.market.scheduler"]