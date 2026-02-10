# db_utils.py
import os
import sys
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session

# Add DB folder to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'DB'))
from database import SessionLocal, engine, Base
from models import GA4Metric, GSCMetric, IndexingStatus, SEOReport, PreprocessedMetric

# -------------------------
# Create all tables
# -------------------------
def init_db():
    """Create all database tables"""
    Base.metadata.create_all(bind=engine)
    print("✅ Database tables created successfully")


# -------------------------
# Store GA4 CSV data
# -------------------------
def store_ga4_csv(csv_path: str, date_col: str = "date", db: Session = None, default_date=None):
    """
    Store GA4 CSV data into database
    
    Args:
        csv_path: Path to GA4 CSV file
        date_col: Name of the date column
        db: Database session (optional)
        default_date: Default date to use if date column missing
    """
    should_close = False
    if db is None:
        db = SessionLocal()
        should_close = True
    
    try:
        df = pd.read_csv(csv_path, engine="python", on_bad_lines="skip")
        if df.empty:
            print(f"⚠️ Empty CSV file: {csv_path}")
            return 0
        
        # Normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        
        # Handle missing date column - use default date
        if date_col not in df.columns:
            if default_date is None:
                default_date = datetime.now().date()
            print(f"ℹ️ No date column in {os.path.basename(csv_path)}, using {default_date}")
            df[date_col] = default_date
        else:
            # Convert date to proper format
            df[date_col] = pd.to_datetime(df[date_col]).dt.date
        
        count = 0
        for _, row in df.iterrows():
            metric = GA4Metric(
                date=row.get(date_col),
                page=str(row.get("page", row.get("landing_page", row.get("page_title", "")))),
                users=int(row.get("users", row.get("total_users", 0))),
                sessions=int(row.get("sessions", 0)),
                engaged_sessions=int(row.get("engaged_sessions", 0)),
                engagement_rate=float(row.get("engagement_rate", 0.0)),
                bounce_rate=float(row.get("bounce_rate", 0.0)),
                average_session_duration=float(row.get("average_session_duration", 0.0)),
                event_count=int(row.get("event_count", 0)),
                conversions=int(row.get("conversions", 0)),
                total_revenue=float(row.get("total_revenue", 0.0))
            )
            db.add(metric)
            count += 1
        
        db.commit()
        print(f"✅ Stored {count} GA4 records from {os.path.basename(csv_path)}")
        return count
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error storing GA4 data from {csv_path}: {e}")
        return 0
    finally:
        if should_close:
            db.close()


# -------------------------
# Store GSC CSV data
# -------------------------
def store_gsc_csv(csv_path: str, date_col: str = "date", db: Session = None):
    """
    Store GSC CSV data into database
    
    Args:
        csv_path: Path to GSC CSV file
        date_col: Name of the date column
        db: Database session (optional)
    """
    should_close = False
    if db is None:
        db = SessionLocal()
        should_close = True
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"⚠️ Empty CSV file: {csv_path}")
            return 0
        
        # Normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        
        # Ensure date column exists
        if date_col not in df.columns:
            print(f"⚠️ Date column '{date_col}' not found in {csv_path}")
            return 0
        
        # Convert date to proper format
        df[date_col] = pd.to_datetime(df[date_col]).dt.date
        
        count = 0
        for _, row in df.iterrows():
            metric = GSCMetric(
                date=row.get(date_col),
                page=str(row.get("page", row.get("url", ""))),
                query=str(row.get("query", None)) if pd.notna(row.get("query")) else None,
                clicks=int(row.get("clicks", 0)),
                impressions=int(row.get("impressions", 0)),
                ctr=float(row.get("ctr", 0.0)),
                position=float(row.get("position", 0.0))
            )
            db.add(metric)
            count += 1
        
        db.commit()
        print(f"✅ Stored {count} GSC records from {os.path.basename(csv_path)}")
        return count
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error storing GSC data from {csv_path}: {e}")
        return 0
    finally:
        if should_close:
            db.close()


# -------------------------
# Store Indexing Status CSV data
# -------------------------
def store_indexing_csv(csv_path: str, date_col: str = "date", db: Session = None):
    """
    Store URL indexing status CSV data into database
    
    Args:
        csv_path: Path to indexing CSV file
        date_col: Name of the date column
        db: Database session (optional)
    """
    should_close = False
    if db is None:
        db = SessionLocal()
        should_close = True
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"⚠️ Empty CSV file: {csv_path}")
            return 0
        
        # Normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        
        count = 0
        for _, row in df.iterrows():
            # Handle date
            date_val = datetime.now().date()
            if date_col in df.columns and pd.notna(row.get(date_col)):
                date_val = pd.to_datetime(row.get(date_col)).date()
            
            # Handle last_crawl_time
            last_crawl = None
            if "last_crawl_time" in df.columns and pd.notna(row.get("last_crawl_time")):
                try:
                    last_crawl = pd.to_datetime(row.get("last_crawl_time"))
                except:
                    pass
            
            metric = IndexingStatus(
                date=date_val,
                url=str(row.get("url", row.get("page", ""))),
                verdict=str(row.get("verdict")) if pd.notna(row.get("verdict")) else None,
                coverage_state=str(row.get("coverage_state")) if pd.notna(row.get("coverage_state")) else None,
                crawled_as=str(row.get("crawled_as")) if pd.notna(row.get("crawled_as")) else None,
                indexing_state=str(row.get("indexing_state")) if pd.notna(row.get("indexing_state")) else None,
                last_crawl_time=last_crawl,
                page_fetch_state=str(row.get("page_fetch_state")) if pd.notna(row.get("page_fetch_state")) else None,
                robots_txt_state=str(row.get("robots_txt_state")) if pd.notna(row.get("robots_txt_state")) else None,
                http_status=int(row.get("http_status")) if pd.notna(row.get("http_status")) else None,
                lcp=float(row.get("lcp")) if pd.notna(row.get("lcp")) else None,
                inp=float(row.get("inp")) if pd.notna(row.get("inp")) else None,
                cls=float(row.get("cls")) if pd.notna(row.get("cls")) else None
            )
            db.add(metric)
            count += 1
        
        db.commit()
        print(f"✅ Stored {count} indexing records from {os.path.basename(csv_path)}")
        return count
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error storing indexing data from {csv_path}: {e}")
        return 0
    finally:
        if should_close:
            db.close()


# -------------------------
# Store SEO Report Metadata
# -------------------------
def store_report_metadata(week_start, week_end, report_type, pdf_path=None, 
                         csv_paths=None, status='generated', gemini_summary=None, 
                         db: Session = None):
    """
    Store SEO report metadata
    """
    should_close = False
    if db is None:
        db = SessionLocal()
        should_close = True
    
    try:
        import json
        report = SEOReport(
            week_start=week_start,
            week_end=week_end,
            report_type=report_type,
            pdf_path=pdf_path,
            csv_paths=json.dumps(csv_paths) if csv_paths else None,
            status=status,
            gemini_summary=gemini_summary
        )
        db.add(report)
        db.commit()
        print(f"✅ Stored report metadata for {week_start} to {week_end}")
        return report.id
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error storing report metadata: {e}")
        return None
    finally:
        if should_close:
            db.close()


# -------------------------
# Batch store all CSV files in a directory
# -------------------------
def store_all_csvs_in_directory(directory: str, db: Session = None):
    """
    Automatically detect and store all CSV files in a directory
    """
    should_close = False
    if db is None:
        db = SessionLocal()
        should_close = True
    
    try:
        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        
        for csv_file in csv_files:
            csv_path = os.path.join(directory, csv_file)
            filename_lower = csv_file.lower()
            
            # Detect file type and store accordingly
            if 'ga4' in filename_lower or 'landing' in filename_lower or 'engagement' in filename_lower:
                store_ga4_csv(csv_path, db=db)
            elif 'gsc' in filename_lower or 'performance' in filename_lower or 'pages' in filename_lower:
                store_gsc_csv(csv_path, db=db)
            elif 'indexing' in filename_lower or 'url_indexing' in filename_lower:
                store_indexing_csv(csv_path, db=db)
            else:
                print(f"⏭️ Skipped unknown file type: {csv_file}")
        
        print(f"✅ Processed all CSV files in {directory}")
        
    except Exception as e:
        print(f"❌ Error processing directory {directory}: {e}")
    finally:
        if should_close:
            db.close()