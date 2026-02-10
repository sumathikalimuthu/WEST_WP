# 🌐 WESTWP – Automated SEO Reporting System

A complete backend automation system that fetches **Google Analytics 4 (GA4)** and  
**Google Search Console (GSC)** data, processes insights, generates reports, and  
emails SEO analytics automatically using **Celery pipelines**.

---

## 🚀 Overview

WESTWP is a **Celery-based automation pipeline** designed to remove manual SEO reporting work.

The system:
- Collects raw GA4 & GSC data
- Generates CSV reports
- Preprocesses and analyzes insights
- Uses AI (Gemini) for summaries
- Converts results into PDFs
- Automatically emails reports

Everything runs on **scheduled background jobs** without manual intervention.

---

## 🎯 Core Features

### 🔹 Data Collection (Raw Reports)
- Fetch GA4 metrics (users, sessions, traffic sources, devices, countries)
- Fetch GSC metrics (queries, pages, clicks, impressions, CTR)
- Export raw data as CSV
- Automatic email delivery

Handled by: **celery_app → seo_tasks**

---

### 🔹 AI Processing & PDF Reports
- Preprocess raw CSV files
- Clean & transform datasets
- Generate AI insights using Gemini
- Convert insights into structured PDF reports
- Email final reports

Handled by: **celery_pdf_app**

---

### 🔹 Automated Scheduling
- Weekly SEO report → Monday 9:00 AM
- Daily indexing data → Every day 9:00 AM
- Fully automated using Celery Beat

---

## 🏗️ Celery Pipelines Architecture

### 🔹 celery_app
Responsible for:
- Running `seo_tasks`
- Reading GA4 & GSC data
- Generating raw CSV files
- Sending CSV attachments via email

### 🔹 celery_pdf_app
Responsible for:
- Preprocessing CSV files
- Calling Gemini AI for insights
- Converting responses into PDF reports
- Sending final PDF reports via Brevo mail

### 🔹 celery beat
Responsible for:
- Weekly scheduled report generation
- Daily indexing report triggers
- Automated background execution

---

## 🛠️ Technology Stack

| Component | Technology |
|-----------|-----------|
| Task Queue | Celery |
| Scheduler | Celery Beat |
| Data Processing | Pandas, NumPy |
| AI Insights | Gemini AI |
| Email Service | Brevo SMTP |
| Google APIs | GA4, GSC |
| Storage | CSV & PDF reports |
| Language | Python 3.x |

---

## ⚙️ How It Works

1. Celery Beat triggers scheduled jobs  
2. celery_app collects GA4 & GSC data → saves CSV  
3. celery_pdf_app preprocesses data → generates AI insights  
4. Reports converted to PDF  
5. Emails automatically sent to stakeholders  

---

## 📌 Use Case

This system is designed for:
- SEO teams
- Marketing teams
- Agencies
- Automated analytics workflows

It eliminates manual downloading, analysis, and reporting.

---

## 🔐 Note

This repository is a simplified version of my internship project.  
Sensitive credentials and confidential data have been removed.