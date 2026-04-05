FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY model_server ./model_server
COPY utils ./utils
COPY configs ./configs

EXPOSE 8000

CMD ["uvicorn", "model_server.app:app", "--host", "0.0.0.0", "--port", "8000"]