FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY configs/ configs/
COPY pipelines/ pipelines/

CMD ["python", "-m", "pipelines.serving.scheduler"]