# 🌐 WESTWP – Automated SEO Reporting System  
A complete backend system that automatically fetches **Google Analytics 4 (GA4)** and  
**Google Search Console (GSC)** data and emails daily/weekly SEO reports using Celery tasks.

---

## 🚀 Overview  
WESTWP is a **FastAPI + Celery** application that automates SEO analytics.  
It connects to GA4 and GSC using a Google Service Account, generates CSV/PDF reports, and  
sends them via email to users automatically (daily or weekly).

This completely removes manual downloading, exporting, and sending of SEO reports.

---

## 🎯 Features  

### 🔹 GA4 Automation  
Fetch total users, sessions, new users  
Fetch top countries, devices, traffic sources  
Fetch page views and performance metrics  
Export data as CSV  

### 🔹 GSC Automation  
Fetch top pages  
Fetch top queries  
Fetch impressions, CTR, clicks  
Export data as CSV  

### 🔹 Email Automation  
Automatically send reports to any email  
Daily/weekly cron scheduling  
Attach CSV files  
Custom email template  

### 🔹 Task Management  
Celery worker for background tasks  
Celery beat for scheduling  
Error handling & retry logic  

### 🔹 API (FastAPI)  
/run-report – Manually trigger report  
/health – Check if API running  
/download/* – Download reports  

---

## 🏗️ Technology Stack  

| Component | Technology |
|----------|------------|
| Backend API | FastAPI |
| Task Queue | Celery |
| Scheduler | Celery Beat |
| Email Service | Brevo / SMTP |
| Google APIs | GA4, GSC |
| Auth | Service Account JSON |
| Storage | Local CSV files |
| Language | Python 3.x |

---

Note: This is a simplified version of my internship project. 
Sensitive credentials and confidential data have been removed.
