import os
from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "seo_tasks",
    broker=REDIS_URL,
    backend=REDIS_URL
)

# 🔥 FORCE LOAD TASK MODULE (THIS IS THE FIX)
import tasks.seo_tasks   # ✅ DO NOT REMOVE

celery_app.conf.timezone = "Asia/Kolkata"

QUEUE_NAME = "seo_reports"

celery_app.conf.beat_schedule = {
    "fetch_seo_report": {
        "task": "tasks.seo_tasks.fetch_and_email_report",
        "schedule": crontab(minute="*/15"),
        "options": {"queue": QUEUE_NAME},
    }
}

celery_app.conf.task_routes = {
    "tasks.seo_tasks.fetch_and_email_report": {"queue": QUEUE_NAME}
}

# 🔥 IMMEDIATE TRIGGER ON WORKER START
@celery_app.on_after_configure.connect
def trigger_immediately(sender, **kwargs):
    sender.send_task("tasks.seo_tasks.fetch_and_email_report", queue=QUEUE_NAME)
