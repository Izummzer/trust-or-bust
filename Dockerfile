# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# зависимости отдельно — лучше кешируются
COPY bot/requirements.txt /app/bot/requirements.txt
RUN pip install --no-cache-dir -r /app/bot/requirements.txt

# код
COPY . /app

ENV PYTHONUNBUFFERED=1

CMD ["python", "bot/app.py"]
