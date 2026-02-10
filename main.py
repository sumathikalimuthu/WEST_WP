# main.py
import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from celery_pdf_app import celery_app

app = FastAPI(title="SEO Report + PDF Trigger API")

OUTPUT_DIR = os.path.join(os.getcwd(), os.getenv("OUTPUT_DIR", "output"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------
# Trigger PDF Report via Celery
# -------------------------
@app.get("/trigger-pdf-report")
@app.post("/trigger-pdf-report")
def trigger_pdf_report():
    """
    Trigger the Celery task that:
      - Preprocesses GA4 & GSC CSVs
      - Generates SEO PDF
      - Sends email
    """
    try:
        task = celery_app.send_task("tasks.pdf_tasks.generate_pdf_report")
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "PDF generation task has been queued",
                "task_id": task.id
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

# -------------------------
# Health Check / Root
# -------------------------
@app.get("/")
def root():
    return {"message": "SEO PDF Report Service Running 🚀"}

# -------------------------
# Optional: Run weekly report (existing GA4 + GSC fetch)
# -------------------------
@app.get("/run-report")
@app.post("/run-report")
def run_report():
    """
    Trigger fetching raw GA4 + GSC CSV reports.
    This can call your existing fetch_and_email_report task if needed.
    """
    try:
        task = celery_app.send_task("tasks.fetch_and_email_report")
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "GA4 & GSC fetch task queued",
                "task_id": task.id
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )