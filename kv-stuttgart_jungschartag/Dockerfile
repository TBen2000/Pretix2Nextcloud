# syntax=docker/dockerfile:1

FROM python:3.12-slim

LABEL org.opencontainers.image.source=https://github.com/TBen2000/Pretix2Nextcloud

WORKDIR /app

RUN python -m venv /app/venv
RUN /app/venv/bin/pip install --upgrade pip

COPY requirements.txt .
RUN /app/venv/bin/pip install --no-cache-dir -r requirements.txt

COPY kv-stuttgart-jungschartag.py .

CMD ["/app/venv/bin/python", "kv-stuttgart-jungschartag.py"]
