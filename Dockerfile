FROM python:3.11
# FROM python:3.11-slim

# Корневые сертификаты для валидации SSL
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Зависимости
COPY bot/requirements.txt /app/bot/requirements.txt
RUN pip install --no-cache-dir -r /app/bot/requirements.txt

# Код
COPY . /app

# Явно укажем путь к системному CA-бандлу (иногда помогает)
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV PYTHONUNBUFFERED=1

CMD ["python", "bot/app.py"]
