FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/common /app/common
COPY src/workers/low_amount_reducer /app/worker

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-u", "/app/worker/main.py"]
