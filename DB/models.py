# models.py
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text, Boolean
from sqlalchemy.sql import func
import sys
import os

# Add DB folder to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'DB'))
from database import Base

# -------------------------
# GA4 Metrics Table
# -------------------------
class GA4Metric(Base):
    __tablename__ = "ga4_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, index=True)
    page = Column(Text, nullable=False, index=True)
    users = Column(Integer, default=0)
    sessions = Column(Integer, default=0)
    engaged_sessions = Column(Integer, default=0)
    engagement_rate = Column(Float, default=0.0)
    bounce_rate = Column(Float, default=0.0)
    average_session_duration = Column(Float, default=0.0)
    event_count = Column(Integer, default=0)
    conversions = Column(Integer, default=0)
    total_revenue = Column(Float, default=0.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# -------------------------
# GSC Performance Table
# -------------------------
class GSCMetric(Base):
    __tablename__ = "gsc_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, index=True)
    page = Column(Text, nullable=False, index=True)
    query = Column(Text, nullable=True)
    clicks = Column(Integer, default=0)
    impressions = Column(Integer, default=0)
    ctr = Column(Float, default=0.0)
    position = Column(Float, default=0.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# -------------------------
# URL Indexing Status Table
# -------------------------
class IndexingStatus(Base):
    __tablename__ = "indexing_status"
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, index=True)
    url = Column(Text, nullable=False, index=True)
    verdict = Column(String(100), nullable=True)
    coverage_state = Column(String(100), nullable=True)
    crawled_as = Column(String(100), nullable=True)
    indexing_state = Column(String(100), nullable=True)
    last_crawl_time = Column(DateTime(timezone=True), nullable=True)
    page_fetch_state = Column(String(100), nullable=True)
    robots_txt_state = Column(String(100), nullable=True)
    http_status = Column(Integer, nullable=True)
    lcp = Column(Float, nullable=True)
    inp = Column(Float, nullable=True)
    cls = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# -------------------------
# SEO Reports Metadata Table
# -------------------------
class SEOReport(Base):
    __tablename__ = "seo_reports"
    
    id = Column(Integer, primary_key=True, index=True)
    week_start = Column(Date, nullable=False)
    week_end = Column(Date, nullable=False)
    report_type = Column(String(50), nullable=False)  # 'weekly', 'monthly', etc.
    pdf_path = Column(Text, nullable=True)
    csv_paths = Column(Text, nullable=True)  # JSON string of file paths
    status = Column(String(50), default='generated')  # 'generated', 'sent', 'failed'
    gemini_summary = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    sent_at = Column(DateTime(timezone=True), nullable=True)


# -------------------------
# Preprocessed Data Table (Optional)
# -------------------------
class PreprocessedMetric(Base):
    __tablename__ = "preprocessed_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, index=True)
    page = Column(Text, nullable=False, index=True)
    total_clicks = Column(Integer, default=0)
    total_impressions = Column(Integer, default=0)
    avg_ctr = Column(Float, default=0.0)
    avg_position = Column(Float, default=0.0)
    avg_lcp = Column(Float, nullable=True)
    avg_inp = Column(Float, nullable=True)
    avg_cls = Column(Float, nullable=True)
    errors = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())