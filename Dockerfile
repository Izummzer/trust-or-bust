FROM python:3.11-slim

# 1) корневые сертификаты для SSL
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2) зависимости
COPY bot/requirements.txt /app/bot/requirements.txt
RUN pip install --no-cache-dir -r /app/bot/requirements.txt

# 3) код
COPY . /app

ENV PYTHONUNBUFFERED=1

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

CMD ["python", "bot/app.py"]
