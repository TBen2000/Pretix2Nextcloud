# syntax=docker/dockerfile:1

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kv-stuttgart-jungschartag.py .
CMD ["python", "kv-stuttgart-jungschartag.py"]