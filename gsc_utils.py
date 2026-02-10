# gsc_utils.py
import os
import time
import csv
import requests
import pandas as pd
from lxml import etree
from io import BytesIO
from google.oauth2 import service_account
from googleapiclient.discovery import build
import logging
from random import uniform
from datetime import datetime
import glob
from urllib.parse import urlparse
import re
from time import sleep
# -------------------------
# GLOBAL CONFIG
# -------------------------
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
INSPECTION_SCOPE = "https://www.googleapis.com/auth/webmasters"

SITE = os.getenv("GSC_SITE_URL", "https://www.smackcoders.com")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")

SERVICE_ACCOUNT_FILE = os.getenv(
    "SERVICE_ACCOUNT_FILE",
    os.path.join(os.getcwd(), "service_account.json")
)

DAILY_INSPECTION_LIMIT = 600

os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------
# LOGGING (SAFE)
# -------------------------
logger = logging.getLogger("gsc_utils")
logger.setLevel(logging.INFO)

log_path = os.path.join(OUTPUT_DIR, "gsc_utils.log")

if not logger.handlers:
    fh = logging.FileHandler(log_path, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

# -------------------------
# CREDENTIALS
# -------------------------
def _load_gsc_credentials(service_account_file):
    return service_account.Credentials.from_service_account_file(
        service_account_file, scopes=SCOPES
    )

def _load_inspection_credentials(service_account_file):
    return service_account.Credentials.from_service_account_file(
        service_account_file, scopes=[INSPECTION_SCOPE]
    )

# -------------------------
# CLOUDFLARE-SAFE ROBOTS.TXT
# -------------------------
def fetch_robots_txt(site_url, timeout=15):
    robots_url = f"{site_url.rstrip('/')}/robots.txt"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/plain,text/html;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

    try:
        resp = requests.get(
            robots_url,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
        )

        if resp.status_code == 200 and resp.text.strip():
            logger.info("✅ robots.txt fetched (Cloudflare-safe)")
            return resp.text

        logger.warning(f"robots.txt not usable (status={resp.status_code})")
        return None

    except Exception as e:
        logger.warning(f"robots.txt skipped: {e}")
        return None


# -------------------------
# HTTP STATUS
# -------------------------
def fetch_http_status(url):
    try:
        return requests.head(url, timeout=15, allow_redirects=True).status_code
    except:
        return None


# -------------------------
# CORE WEB VITALS (PSI)
# -------------------------
PSI_API_URL = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

def fetch_cwv(url, api_key):
    params = {
        "url": url,
        "strategy": "mobile",
        "category": "performance",
        "key": api_key
    }

    try:
        r = requests.get(PSI_API_URL, params=params, timeout=120)
        r.raise_for_status()
        audits = r.json()["lighthouseResult"]["audits"]

        def metric(k):
            return audits.get(k, {}).get("numericValue")

        return {
            "lcp": metric("largest-contentful-paint"),
            "inp": metric("interaction-to-next-paint"),
            "cls": metric("cumulative-layout-shift"),
            "fcp": metric("first-contentful-paint"),
        }

    except Exception as e:
        logger.warning(f"CWV failed for {url}: {e}")
        return {"lcp": None, "inp": None, "cls": None, "fcp": None}


# -------------------------
# PERFORMANCE API
# -------------------------
def run_gsc_query(service, site_url, start_date, end_date, dimensions):
    try:
        body = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "rowLimit": 25000
        }
        response = service.searchanalytics().query(
            siteUrl=site_url, body=body
        ).execute()
        return response.get("rows", []) if isinstance(response, dict) else []
    except Exception as e:
        logger.warning(f"Error fetching {dimensions}: {e}")
        return []

def save_csv(file_path, headers, rows):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        if not rows:
            writer.writerow(["No data"])
        else:
            for row in rows:
                keys = row.get("keys", [])
                writer.writerow(keys + [
                    row.get("clicks", 0),
                    row.get("impressions", 0),
                    row.get("ctr", 0.0),
                    row.get("position", 0.0)
                ])

def fetch_gsc_performance_full(service_account_file, site_url, output_dir, start_date, end_date):
    logger.info(f"Fetching GSC Performance Reports for {site_url}")
    creds = _load_gsc_credentials(service_account_file)
    service = build("searchconsole", "v1", credentials=creds)

    perf_dir = os.path.join(output_dir, "GSC Reports", "Performance Reports")
    os.makedirs(perf_dir, exist_ok=True)

    report_tables = [
        ("Top pages.csv", ["Top pages", "Clicks", "Impressions", "CTR", "Position"],
         run_gsc_query(service, site_url, start_date, end_date, ["page"]))
    ]

    saved = []
    for name, headers, rows in report_tables:
        path = os.path.join(perf_dir, name)
        save_csv(path, headers, rows)
        saved.append(path)

    return saved

# -------------------------
# SITEMAP FETCH
# -------------------------
def fetch_sitemap_urls(site_url=SITE, output_dir=OUTPUT_DIR):
    robots_txt = fetch_robots_txt(site_url)
    if not robots_txt:
          logger.info("Proceeding without robots.txt (safe)")


    all_urls = set()
    visited_sitemaps = set()
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    def fetch_urls_from_sitemap(sitemap_url):
        if sitemap_url in visited_sitemaps:
            return
        visited_sitemaps.add(sitemap_url)

        try:
            res = requests.get(sitemap_url, timeout=30)
            tree = etree.parse(BytesIO(res.content))
        except Exception as e:
            logger.warning(f"Failed to parse sitemap {sitemap_url}: {e}")
            return

        child_sitemaps = tree.xpath("//sm:sitemap/sm:loc/text()", namespaces=ns)
        if child_sitemaps:
            for child in child_sitemaps:
                fetch_urls_from_sitemap(child)
        else:
            urls = tree.xpath("//sm:url/sm:loc/text()", namespaces=ns)
            all_urls.update(urls)

    # FIX 3: sitemap_urls defined
    sitemap_urls = [f"{site_url}/sitemap.xml"]

    for sitemap in sitemap_urls:
        fetch_urls_from_sitemap(sitemap)

    df = pd.DataFrame(sorted(all_urls), columns=["url"])

    indexing_dir = os.path.join(output_dir, "indexing reports")
    os.makedirs(indexing_dir, exist_ok=True)

    path = os.path.join(indexing_dir, "sitemap_pages.csv")
    df.to_csv(path, index=False)

    logger.info(f"Sitemap URLs fetched: {len(df)}")
    return path

# -------------------------
# FILTER
# -------------------------
def filter_sitemap_urls(pages_csv, output_dir=OUTPUT_DIR):
    df = pd.read_csv(pages_csv)

    if df.empty:
        logger.warning("Sitemap CSV empty")
        return pages_csv
    

    bad_patterns = ["?page=", "/tag/", "/author/"]

    df = df[~df["url"].str.contains(
        "|".join(bad_patterns),
        regex=False,
        na=False
    )]

    path = os.path.join(output_dir, "indexing reports", "filtered_pages.csv")
    df.to_csv(path, index=False)

    logger.info(f"Filtered sitemap URLs: {len(df)}")
    return path



# -------------------------
# URL INSPECTION
# -------------------------
def inspect_urls(service_account_file, site_url, filtered_csv, output_dir=OUTPUT_DIR):
    creds = _load_inspection_credentials(service_account_file)
    service = build("searchconsole", "v1", credentials=creds)

    df = pd.read_csv(filtered_csv)
    rows = []
    missing_urls = []
    inspected_today = 0

    robots_txt = fetch_robots_txt(site_url)
    if not robots_txt:
        logger.info("robots.txt unavailable – inspection continues safely")


    

    for url in df["url"]:
        if inspected_today >= DAILY_INSPECTION_LIMIT:
            break

       

        logger.info(f"Inspecting URL: {url}")
        try:
            resp = service.urlInspection().index().inspect(
                body={"inspectionUrl": url, "siteUrl": site_url}
            ).execute()

            result = resp["inspectionResult"]["indexStatusResult"]

            rows.append({
                "url": url,
                "coverage_state": result.get("coverageState"),
                "indexing_state": result.get("indexingState"),
                "last_crawl": result.get("lastCrawlTime"),
                "verdict": result.get("verdict")
            })

            inspected_today += 1
            time.sleep(uniform(0.2, 0.5))

        except Exception as e:
            logger.warning(f"Inspection failed: {url} ({e})")
            missing_urls.append(url)

    today = datetime.now().strftime("%Y-%m-%d")
    out = os.path.join(output_dir, f"url_indexing_status_{today}.csv")
    pd.DataFrame(rows).to_csv(out, index=False)

    logger.info(f"Saved daily inspection CSV: {out}")
    return out

# -------------------------
# WEEKLY COMBINE
# -------------------------
def combine_weekly_indexing_status(output_dir=OUTPUT_DIR):
    files = glob.glob(os.path.join(output_dir, "url_indexing_status_*.csv"))
    if not files:
        return None

    df_list = []
    for f in files:
        df = pd.read_csv(f)
        if df.empty:
            continue
        date = os.path.basename(f).replace("url_indexing_status_", "").replace(".csv", "")
        df["inspection_date"] = pd.to_datetime(date)
        df_list.append(df)

    combined = pd.concat(df_list, ignore_index=True)
    combined.sort_values("inspection_date", ascending=False, inplace=True)
    master = combined.drop_duplicates("url")

    master_path = os.path.join(output_dir, "url_indexing_status.csv")
    master.drop(columns=["inspection_date"]).to_csv(master_path, index=False)

    logger.warning(f"🎉 MASTER FILE CREATED: {master_path}")
    logger.warning(f"Final shape: {master.shape}")

    return master_path




def normalize_url(url):
    if pd.isna(url):
        return url
    parsed = urlparse(url.strip().lower())
    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    return clean.rstrip("/")

# -------------------------
# MERGE WITH PERFORMANCE
# -------------------------
def merge_indexing_with_performance(indexing_csv, output_dir=OUTPUT_DIR):
    indexing = pd.read_csv(indexing_csv)

    perf_csv = os.path.join(
        output_dir, "GSC Reports", "Performance Reports", "Top pages.csv"
    )
    gsc = pd.read_csv(perf_csv)

    # Rename column
    gsc.rename(columns={"Top pages": "url"}, inplace=True)

    #  NORMALIZATION
    indexing["url_norm"] = indexing["url"].apply(normalize_url)
    gsc["url_norm"] = gsc["url"].apply(normalize_url)

    #  MERGE
    merged = pd.merge(
        indexing,
        gsc.drop(columns=["url"]),
        on="url_norm",
        how="left"
    )

    # Cleanup
    merged.drop(columns=["url_norm"], inplace=True)

    out = os.path.join(output_dir, "final_pages_indexing_performance.csv")
    merged.to_csv(out, index=False)

    return out

# -------------------------
# MERGE CWV WITH FINAL INDEXING CSV
# -------------------------

def merge_cwv_with_indexing(final_csv, output_dir=OUTPUT_DIR):
    df = pd.read_csv(final_csv)

    PSI_API_KEY = os.getenv("PSI_API_KEY")
    if not PSI_API_KEY:
        logger.warning("PSI_API_KEY missing")
        return final_csv

    rows = []

    for url in df["url"]:
        http_status = fetch_http_status(url)
        cwv = fetch_cwv(url, PSI_API_KEY)

        if not cwv:
            logger.info(f"CWV skipped for {url}")
            cwv = {}

        rows.append({
            "url": url,
            "http_status": http_status,
            "lcp": cwv.get("lcp"),
            "inp": cwv.get("inp"),
            "cls": cwv.get("cls"),
            "fcp": cwv.get("fcp")
        })

        time.sleep(uniform(1, 2))

    cwv_df = pd.DataFrame(rows)

    merged = pd.merge(
        df,
        cwv_df,
        on="url",
        how="left"
    )

    out = os.path.join(output_dir, "final_pages_indexing_performance_cwv.csv")
    merged.to_csv(out, index=False)

    logger.info(f"FINAL INDEXING + CWV FILE CREATED: {out}")
    return out



# -------------------------
#  SMART INDEXING PIPELINE
# -------------------------
def run_gsc_indexing_pipeline(
    service_account_file,
    site_url,
    output_dir=OUTPUT_DIR
):
    logger.info("🚀 Starting GSC Indexing Pipeline")

    # 1️⃣ Fetch sitemap URLs
    sitemap_csv = fetch_sitemap_urls(
        site_url=site_url,
        output_dir=output_dir
    )

    # 2️⃣ Filter sitemap URLs
    filtered_csv = filter_sitemap_urls(
        pages_csv=sitemap_csv,
        output_dir=output_dir
    )

    # 3️⃣ URL Inspection (creates daily url_indexing_status_*.csv)
    inspect_urls(
        service_account_file=service_account_file,
        site_url=site_url,
        filtered_csv=filtered_csv,
        output_dir=output_dir
    )

    # 4️⃣ Combine weekly/daily indexing status
    master_csv = combine_weekly_indexing_status(
        output_dir=output_dir
    )

    # 5️⃣ Merge indexing + GSC performance
    if master_csv:
        final_csv = merge_indexing_with_performance(
            indexing_csv=master_csv,
            output_dir=output_dir
        )

        # 6️⃣ Merge CWV LAST (correct place)
        if final_csv:
            merge_cwv_with_indexing(
                final_csv=final_csv,
                output_dir=output_dir
            )

    logger.info("✅ GSC Indexing Pipeline completed")
    return master_csv



# -------------------------
# 🔄 COMBINED FETCH FUNCTION (for Celery)
# -------------------------
def fetch_gsc_full(
    service_account_file,
    project_id,
    site_url,
    start_date,
    end_date,
    base_output_dir
):
    """
    This function is called from Celery (seo_tasks.py)
    DO NOT change signature
    """

    perf_files = fetch_gsc_performance_full(
        service_account_file=service_account_file,
        site_url=site_url,
        output_dir=base_output_dir,
        start_date=start_date,
        end_date=end_date
    )

    final_csv = run_gsc_indexing_pipeline(
        service_account_file=service_account_file,
        site_url=site_url,
        output_dir=base_output_dir
    )

    logger.info("✅GSC Performance + Indexing completed")

    return perf_files + ([final_csv] if final_csv else [])

