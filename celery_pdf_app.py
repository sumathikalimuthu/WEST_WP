import os
from celery import Celery
from dotenv import load_dotenv
from celery.schedules import crontab
import sys

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# -------------------------
# Redis URL
# -------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# -------------------------
# Celery App
# -------------------------
celery_pdf_app = Celery(
    "seo_pdf_tasks",
    broker=REDIS_URL,
    backend=REDIS_URL
)

celery_pdf_app.conf.timezone = "Asia/Kolkata"

# Dedicated queue
QUEUE_NAME = "seo_pdf_reports"
celery_pdf_app.conf.task_routes = {
    "tasks.generate_pdf_report": {"queue": QUEUE_NAME}
}

# Auto-discover tasks
celery_pdf_app.autodiscover_tasks(["tasks.pdf_tasks"])

# -------------------------
# Beat schedule (weekly)
# -------------------------
celery_pdf_app.conf.beat_schedule = {
    "weekly_pdf_report": {
        "task": "tasks.generate_pdf_report",
        "schedule": crontab(hour=9, minute=0, day_of_week=1),  # Every Monday 9:00 AM
        "options": {"queue": QUEUE_NAME}
    }
}

