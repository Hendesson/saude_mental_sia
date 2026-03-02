FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data assets

EXPOSE 8050

ENV PYTHONUNBUFFERED=1
ENV PORT=8050

CMD sh -c "gunicorn app:server --bind 0.0.0.0:${PORT:-8050} --workers 1 --timeout 300 --graceful-timeout 30 --preload --access-logfile - --error-logfile - --log-level info"
