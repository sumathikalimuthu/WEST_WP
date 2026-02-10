# tasks/seo_tasks.py
import os
import glob
from datetime import date, timedelta
from dotenv import load_dotenv
from celery_app import celery_app   
from ga4_utils import fetch_ga4_full
from gsc_utils import fetch_gsc_full
from send_email import send_email as send_email_util
import time
from io import BytesIO
import pandas as pd
import requests
from lxml import etree
from google.oauth2 import service_account
from googleapiclient.discovery import build

# -------------------------
# Load environment
# -------------------------
load_dotenv()

SERVICE_ACCOUNT_FILE = os.getenv(
    "SERVICE_ACCOUNT_FILE",
    os.path.join(os.getcwd(), "service_account.json")
)
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
GSC_SITE_URL = os.getenv("GSC_SITE_URL")
OUTPUT_DIR = os.path.join(os.getcwd(), os.getenv("OUTPUT_DIR", "output"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

SITE = GSC_SITE_URL
INSPECTION_SCOPE = "https://www.googleapis.com/auth/webmasters"


# -------------------------
# MERGE DAILY FILES INTO WEEKLY
# -------------------------
def merge_daily_indexing_files():
    """
    Merge all daily url_indexing_status_YYYY-MM-DD.csv files into url_indexing_status.csv
    and delete daily files older than 7 days.
    """
    try:
        # Pattern for daily files
        daily_pattern = os.path.join(OUTPUT_DIR, "url_indexing_status_*.csv")
        daily_files = glob.glob(daily_pattern)
        
        if not daily_files:
            print("ℹ️ No daily indexing files found to merge.")
            return
        
        print(f"📂 Found {len(daily_files)} daily indexing files")
        
        # Merge all daily files
        all_data = []
        for file_path in daily_files:
            try:
                df = pd.read_csv(file_path)
                if not df.empty:
                    all_data.append(df)
                    print(f"✅ Read: {os.path.basename(file_path)}")
            except Exception as e:
                print(f"⚠️ Failed to read {file_path}: {e}")
        
        if not all_data:
            print("⚠️ No valid data found in daily files")
            return
        
        # Combine all dataframes
        merged_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates (keep latest entry per URL)
        if 'url' in merged_df.columns:
            merged_df = merged_df.drop_duplicates(subset=['url'], keep='last')
        
        # Save merged file
        weekly_file = os.path.join(OUTPUT_DIR, "url_indexing_status.csv")
        merged_df.to_csv(weekly_file, index=False)
        print(f"✅ Merged file saved: {weekly_file} ({len(merged_df)} rows)")
        
        # -------------------------
        # DELETE DAILY FILES OLDER THAN 7 DAYS
        # -------------------------
        cutoff_date = date.today() - timedelta(days=7)
        deleted_count = 0
        
        for file_path in daily_files:
            try:
                # Extract date from filename: url_indexing_status_2026-01-20.csv
                filename = os.path.basename(file_path)
                date_str = filename.replace("url_indexing_status_", "").replace(".csv", "")
                file_date = date.fromisoformat(date_str)
                
                # Delete if older than 7 days
                if file_date < cutoff_date:
                    os.remove(file_path)
                    deleted_count += 1
                    print(f"🗑️ Deleted old file: {filename} (Date: {file_date})")
                else:
                    print(f"📌 Kept recent file: {filename} (Date: {file_date})")
                    
            except (ValueError, OSError) as e:
                print(f"⚠️ Could not process {file_path}: {e}")
        
        print(f"✅ Cleanup complete: {deleted_count} old files deleted")
        
    except Exception as e:
        print(f"❌ Error in merge_daily_indexing_files: {e}")


# 🔥 CELERY TASK
@celery_app.task(name="tasks.seo_tasks.fetch_and_email_report")
def fetch_and_email_report():
    """Fetch GA4 + GSC reports, sitemap indexing, merge, and send email."""
    try:
        today = date.today()

        # -------------------------
        # GA4 Reports
        # -------------------------
        ga4_end = today - timedelta(days=1)
        ga4_start = ga4_end - timedelta(days=6)
        ga4_files = fetch_ga4_full(
            SERVICE_ACCOUNT_FILE,
            GA4_PROPERTY_ID,
            OUTPUT_DIR,
            start_date=ga4_start.isoformat(),
            end_date=ga4_end.isoformat()
        )

        # -------------------------
        # GSC Performance Reports
        # -------------------------
        gsc_end = today - timedelta(days=2)
        gsc_start = gsc_end - timedelta(days=6)
        gsc_files = fetch_gsc_full(
            SERVICE_ACCOUNT_FILE,
            project_id=None,
            site_url=GSC_SITE_URL,
            start_date=gsc_start.isoformat(),
            end_date=gsc_end.isoformat(),
            base_output_dir=OUTPUT_DIR
        )

        # -------------------------
        # MERGE DAILY INDEXING FILES
        # -------------------------
        print("🔄 Starting daily indexing file merge...")
        merge_daily_indexing_files()

        # -------------------------
        # SEND EMAIL
        # -------------------------
        subject = f"📊 Weekly GA4 & GSC Reports ({ga4_start} → {ga4_end})"
        body = "<p>Attached is the complete set of GA4, GSC, and URL inspection analytics reports.</p>"
        send_email_util(subject=subject, body=body, attachment_dir=OUTPUT_DIR)

        print(f"✅ Task completed: GA4 {len(ga4_files)} files, GSC {len(gsc_files)} files.")
        return {
            "status": "success",
            "ga4_files_count": len(ga4_files),
            "gsc_files_count": len(gsc_files),
            "output_dir": OUTPUT_DIR
        }

    except Exception as exc:
        print("❌ Error in fetch_and_email_report:", exc)
        return {"status": "error", "error": str(exc)}
