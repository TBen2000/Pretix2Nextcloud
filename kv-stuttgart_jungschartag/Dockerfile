# syntax=docker/dockerfile:1

FROM python:3.12-slim

LABEL org.opencontainers.image.source https://github.com/TBen2000/Pretix2Nextcloud

RUN useradd -m p2n_user
USER p2n_user

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kv-stuttgart-jungschartag.py .
CMD ["python", "kv-stuttgart-jungschartag.py"]