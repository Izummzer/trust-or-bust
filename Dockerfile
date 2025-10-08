FROM python:3.11-slim

# системные корневые сертификаты (на всякий случай)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# зависимости
COPY bot/requirements.txt /app/bot/requirements.txt
RUN pip install --no-cache-dir -r /app/bot/requirements.txt

# код + наш CA Supabase
COPY . /app

# укажем путь к нашему сертификату Supabase
ENV SUPABASE_CA=/app/bot/certs/prod-ca-2021.crt
# (оставим и системный бандл на всякий)
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV PYTHONUNBUFFERED=1

CMD ["python", "bot/app.py"]
