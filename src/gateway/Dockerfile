FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/common /app/common
COPY src/gateway /app/gateway

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-u", "/app/gateway/main.py"]
