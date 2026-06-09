FROM docker:27-cli AS dockercli

FROM python:3.11-slim

WORKDIR /app

COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/common /app/common
COPY src/watchdog /app/watchdog

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-u", "/app/watchdog/main.py"]
