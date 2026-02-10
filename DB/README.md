
# SEO WestWP – Automated SEO Analysis & Weekly Reporting System

SEO WestWP is an automated SEO system that collects data from
Google Search Console (GSC) and Google Analytics 4 (GA4),
processes it using Celery,
analyzes SEO issues using Gemini AI,
generates weekly PDF SEO reports,
and sends them automatically via Brevo (Sendinblue) email.

Daily data is fetched automatically.
Weekly SEO reports are generated and emailed.

---

## What this project does

**Daily**
  - Fetch GSC data
  - Fetch GA4 data
  - Send raw data email

**Weekly**
  - Clean and merge data
  - Gemini AI SEO analysis
  - Generate PDF report
  - Send PDF via Brevo email

---

## Tech stack

Python
Celery + Celery Beat
Pandas
Google Search Console API
Google Analytics 4 API
Gemini AI
Brevo SMTP (Sendinblue)
PDF generation
FastAPI (Uvicorn)
Redis / RabbitMQ

---

## Prerequisites

Python 3.10+
pip
Redis or RabbitMQ running
Google Service Account (GSC + GA4 access)
Brevo SMTP credentials

---

## Project Structure (with comments)

text
FINAL/
│
├── DB/                         # Raw & processed SEO data
├── output/                     # Daily fetched raw outputs
├── preprocessed_outputs/       # Weekly cleaned & merged data
│
├── preprocessing/
│   └── preprocessing.py        # Weekly data cleaning & merging
│
├── reports/                    # Generated weekly PDF reports
│
├── tasks/
│   ├── seo_tasks.py            # Fetch, preprocess, AI, email tasks
│   └── pdf_tasks.py            # PDF generation tasks
│
├── celery_app.py               # Main Celery app (seo_reports queue)
├── celery_pdf_app.py           # PDF Celery app (seo_pdf_reports queue)
│
├── ga4_utils.py                # GA4 API logic
├── gsc_utils.py                # GSC API logic
├── pdf_utils.py                # PDF helper functions
├── send_email.py               # Brevo email logic
│
├── main.py                     # FastAPI entry point
├── requirements.txt            # Python dependencies
├── service_account.json        # Google API credentials
├── README.md                   # Project documentation
└── .env                        # Environment variables (DO NOT COMMIT)
### 🛠 Installation & Setup

### 1️⃣ Clone the repository
bash
git clone <your_repo_url>
cd FINAL


# Create virtual environment
python -m venv venv

# Activate virtual environment

# Windows
venv\Scripts\activate

# Linux / macOS
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
# 1. Start main Celery worker
# Fetch data, preprocessing, AI analysis, email
celery -A celery_app worker --loglevel=info --pool=solo -Q seo_reports

# 2. Start Celery Beat
# Schedules daily & weekly jobs
celery -A celery_app beat --loglevel=info

# 3. Start PDF worker
# Handles only PDF generation
celery -A celery_pdf_app.celery_pdf_app worker -Q seo_pdf_reports -l info --concurrency=1 --pool=solo

# 4. Run FastAPI server
# API access & monitoring
uvicorn main:app --reload