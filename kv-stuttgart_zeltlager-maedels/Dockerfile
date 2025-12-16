# syntax=docker/dockerfile:1

FROM python:3.12-slim

LABEL org.opencontainers.image.source=https://github.com/TBen2000/Pretix2Nextcloud
LABEL org.opencontainers.image.description="P2N - KV Stuttgart Zeltlager Mädels"
LABEL org.opencontainers.image.licenses=MIT

WORKDIR /app

COPY requirements.txt .
COPY kv-stuttgart_zeltlager-maedels/kv-stuttgart-zeltlager-maedels.py .
COPY P2N.py .

RUN python -m venv /app/venv
RUN /app/venv/bin/pip install --upgrade pip
RUN /app/venv/bin/pip install --no-cache-dir -r requirements.txt

CMD ["/app/venv/bin/python", "kv-stuttgart-zeltlager-maedels.py"]
