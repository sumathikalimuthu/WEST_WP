# init_database.py
"""
Run this script ONCE to create all database tables before running Celery workers
Usage: python init_database.py
"""
from database import engine, Base
from models import GA4Metric, GSCMetric, IndexingStatus, SEOReport, PreprocessedMetric

def create_tables():
    """Create all database tables"""
    try:
        print("🔧 Creating database tables...")
        Base.metadata.create_all(bind=engine)
        print("✅ Database tables created successfully!")
        print("\nCreated tables:")
        print("  - ga4_metrics")
        print("  - gsc_metrics")
        print("  - indexing_status")
        print("  - seo_reports")
        print("  - preprocessed_metrics")
        print("\n🚀 You can now run your Celery workers!")
    except Exception as e:
        print(f"❌ Error creating tables: {e}")

if __name__ == "__main__":
    create_tables()