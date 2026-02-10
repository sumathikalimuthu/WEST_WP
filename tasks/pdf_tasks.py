# tasks/pdf_tasks.py
import os
import sys
import requests
import pandas as pd
from celery_pdf_app import celery_pdf_app
from send_email import send_email
from pdf_utils import generate_seo_pdf

# Add parent directory to path to import preprocessing
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import preprocessing functions - FIXED PATH
# If preprocessing is a folder, import from preprocessing.preprocessing
# If preprocessing.py is in root, this should work
try:
    # Try importing from preprocessing.py file in root
    from preprocessing import (
        normalize_columns,
        detect_seo_errors,
        sort_seo_priority,
        aggregate_page_metrics,
        aggregate_cwv,
        aggregate_errors,
        build_gemini_summary,
        process_file,
        main as run_preprocessing
    )
except ImportError:
    # If that fails, try importing from preprocessing folder
    from preprocessing.preprocessing import (
        normalize_columns,
        detect_seo_errors,
        sort_seo_priority,
        aggregate_page_metrics,
        aggregate_cwv,
        aggregate_errors,
        build_gemini_summary,
        process_file,
        main as run_preprocessing
    )

# -------------------------
# PATHS
# -------------------------
BASE_DIR = r"D:\Final"
PREPROCESSED_DIR = os.path.join(BASE_DIR, "preprocessed_outputs")
OUTPUT_DIR = PREPROCESSED_DIR
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------
# GEMINI CONFIG
# -------------------------
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = "gemini-2.5-flash"
TIMEOUT = 180

# -------------------------
# GEMINI CALL
# -------------------------
def call_gemini(prompt: str) -> str:
    headers = {"Content-Type": "application/json"}
    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
            headers=headers,
            json=payload,
            timeout=TIMEOUT
        )
        r.raise_for_status()
        data = r.json()
        parts = data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
        return parts[0].get("text", "") if parts else ""
    except Exception:
        return ""

# -------------------------
# READ FULL DATA
# -------------------------
def build_safe_dataset(csv_files):
    blocks = []

    for f in csv_files:
        try:
            df = pd.read_csv(f)
            if df.empty:
                continue
            blocks.append(
                f"\n--- FILE: {os.path.basename(f)} ---\n" + df.to_string(index=False)
            )
        except Exception:
            continue

    return "\n".join(blocks)

# -------------------------
# CELERY TASK - NOW INCLUDES PREPROCESSING
# -------------------------
@celery_pdf_app.task(name="tasks.generate_pdf_report")
def generate_pdf_report():
    try:
        # STEP 1: Run preprocessing first
        print("🔄 Running preprocessing...")
        run_preprocessing()  # This runs the entire preprocessing pipeline
        print("✅ Preprocessing completed")
        
        # STEP 2: Now work with preprocessed files
        csv_files = sorted([
            os.path.join(PREPROCESSED_DIR, f)
            for f in os.listdir(PREPROCESSED_DIR)
            if f.endswith(".csv")
        ])

        if not csv_files:
            raise ValueError("No CSV files found")

        full_data = build_safe_dataset(csv_files)

        prompt = f"""
You are a senior SEO consultant.

DATA BELOW IS AGGREGATED GA4 + GSC DATA.

TASK:
Create a CLIENT-READY WEEKLY SEO REPORT with actionable AI recommendations.

RULES:
- Bullet points only
- Short & clear
- Include page/path wherever possible
- Include AI recommendations for each issue
- Provide a priority level (P1=Immediate, P2=High, P3=Medium)
- Suggest an owner (e.g., SEO Team, Dev Team, Content Team)
- Never say "no data" or ask for more input

STRUCTURE:
1. Executive Summary
2. Indexing Issues (CRITICAL – DO NOT SKIP)
3. Core Web Vitals Issues
4. CTR Issues
5. Content Issues
6. Technical SEO Issues
7. Fix Priority Roadmap
8. Final SEO Verdict
9. 🚨 Slow & Underperforming Pages (CRITICAL)

For EACH issue:
- Page / Path (example or group)
- Issue type
- Observed metric
- Impact
- Fix
- AI Recommendation
- Priority (P1/P2/P3)
- Owner

DATA:
{full_data}
"""

        print("🤖 Calling Gemini AI...")
        seo_report = call_gemini(prompt)

        if not seo_report.strip():
            seo_report = """
Executive Summary
- SEO data available but AI insight generation partially limited

Technical SEO Issues
- Site-wide performance and indexing signals require manual validation
"""

        print("📄 Generating PDF...")
        pdf_path = os.path.join(OUTPUT_DIR, "Weekly_SEO_Report.pdf")
        if os.path.exists(pdf_path):
            os.remove(pdf_path)

        generate_seo_pdf(pdf_path, seo_report)

        print("📧 Sending email...")
        
        # Get current date for email body
        from datetime import datetime
        import shutil
        current_date = datetime.now().strftime("%B %d, %Y")  # e.g., "January 27, 2026"
        
        # Create separate folder for email attachments (PDF only)
        EMAIL_DIR = os.path.join(BASE_DIR, "email_attachments")
        os.makedirs(EMAIL_DIR, exist_ok=True)
        
        # Clear previous files and copy only the PDF
        for old_file in os.listdir(EMAIL_DIR):
            os.remove(os.path.join(EMAIL_DIR, old_file))
        
        shutil.copy(pdf_path, EMAIL_DIR)
        print(f"📎 PDF copied to email directory: {EMAIL_DIR}")
        
        email_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2c3e50;">Hello Team,</h2>
            
            <p>This is our <strong>Weekly SEO Report</strong> for the week ending <strong>{current_date}</strong>.</p>
            
            <p>The attached report includes:</p>
            <ul>
                <li>Performance analysis of key pages</li>
                <li>Indexing and Core Web Vitals insights</li>
                <li>Actionable AI-powered recommendations with priorities</li>
            </ul>
            
            <p>Please review the findings and let us know if you need any clarification or additional analysis.</p>
            
            <p style="margin-top: 20px;">Best regards,<br>
            <strong>SEO Analytics Team</strong></p>
        </body>
        </html>
        """
        
        send_email(
            subject=f"Weekly SEO Report - {current_date}",
            body=email_body,
            attachment_dir=EMAIL_DIR  # Only PDF here!
        )

        print("✅ PDF Report generation completed successfully!")
        return {"status": "success", "pdf": pdf_path}

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {"status": "failed", "error": str(e)}
