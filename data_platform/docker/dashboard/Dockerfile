FROM python:3.10-slim

USER root

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/app \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /opt/app

COPY dashboard/requirements.txt /tmp/dashboard-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dashboard-requirements.txt && rm -f /tmp/dashboard-requirements.txt

COPY dashboard/ /opt/app/dashboard/
COPY configs/ /opt/app/configs/

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health')"

CMD ["streamlit", "run", "dashboard/app.py"]
