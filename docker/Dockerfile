FROM python:3.10-slim

WORKDIR /app

# Copy only requirements first for caching
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy everything else
COPY . .

# Default CMD (can be overridden by docker-compose)
CMD ["python", "-m", "celery", "-A", "celery_app", "worker", "--loglevel=info", "--pool=solo", "--autoreload"]
