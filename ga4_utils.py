from datetime import date, timedelta, datetime
import os
import csv
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient, RunReportRequest, DateRange, Dimension, Metric, Filter, FilterExpression
from google.oauth2 import service_account
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    CohortSpec,
    Cohort,
    CohortsRange,
    Metric,
    Dimension,
    DateRange,
)
import pandas as pd
def _load_credentials(service_account_file, scopes=None):
    if scopes:
        return service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
    return service_account.Credentials.from_service_account_file(service_account_file)

def write_csv_from_response(response, filename):
    
    import csv

    if not response or not getattr(response, "rows", None):
        print(f"[WARNING] No data returned for {filename}")
        with open(filename, "w", newline="", encoding="utf-8") as f:
            f.write("No data\n")
        return

    
    headers = [dim.name for dim in response.dimension_headers] + \
              [met.name for met in response.metric_headers]

    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for row in response.rows:
            dims = [d.value for d in row.dimension_values]
            mets = [m.value for m in row.metric_values]
            writer.writerow(dims + mets)
def safe_report(client, property_id, dimensions, metrics, start_date, end_date):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    return client.run_report(request)
def row_to_values(row):
    
    return [v.value for v in row.dimension_values] + [v.value for v in row.metric_values]

def fetch_ga4_acquisition_reports(service_account_file, property_id, output_dir, start_date=None, end_date=None):
    
    today = date.today()
    weekday = today.weekday()  

    if weekday < 4:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = today
    elif weekday == 4:  
        start_date_dt = today - timedelta(days=4)
        end_date_dt = today - timedelta(days=1)
    else:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = start_date_dt + timedelta(days=4)

    start_date = start_date_dt.isoformat()
    end_date = end_date_dt.isoformat()

    print(f" Fetching GA4 Acquisition Reports data from {start_date_dt} to {end_date_dt}")


    acquisition_dir = os.path.join(output_dir, "Acquisition Reports")
    os.makedirs(acquisition_dir, exist_ok=True)

    creds = service_account.Credentials.from_service_account_file(service_account_file)
    client = BetaAnalyticsDataClient(credentials=creds)
    written_files = []

    def write_csv(file_path, headers, rows):
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def safe_report(client, property_id, dimensions, metrics, start_date, end_date):
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=100000
        )
        return client.run_report(request)

    def row_to_values(row):
        return [v.value for v in list(row.dimension_values) + list(row.metric_values)]

    # -----------------------------
    # Acquisition Overview (10 tables)
    # -----------------------------
    overview_file = os.path.join(acquisition_dir, "Acquisition overview.csv")
    with open(overview_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)

        tables = [
    ("Nth day - Active users", ["nthDay"], ["activeUsers"]),
    ("Nth day - New users", ["nthDay"], ["newUsers"]),
    ("First user primary channel group (Default Channel Group) - New users",
     ["firstUserDefaultChannelGroup"], ["newUsers"]),
    ("Page title and screen class - Views", ["pageTitle"], ["screenPageViews"]),
    ("Session primary channel group (Default Channel Group) - Sessions",
     ["sessionDefaultChannelGroup"], ["sessions"]),
    ("Session Google Ads campaign - Sessions", ["sessionCampaignName"], ["sessions"]),

    ("Landing page + query string - Organic Google Search impressions",
     ["landingPagePlusQueryString"], ["organicGoogleSearchImpressions"]),


    ("Organic Google Search query - Organic Google Search clicks",
     ["landingPagePlusQueryString"], ["organicGoogleSearchClicks"]),

    ("Session manual source - Sessions", ["sessionSource"], ["sessions"]),
]


        for title, dims, mets in tables:
            f.write(f"Table: {title}\n")
            try:
                resp = safe_report(client, property_id, dims, mets, start_date, end_date)
                if not resp.rows:
                    writer.writerow(["No data"])
                else:
                    rows = [row_to_values(row) for row in resp.rows]
                    df = pd.DataFrame(rows, columns=dims + mets)
                    df.to_csv(f, index=False)
            except Exception as e:
                print(f"⚠ Error fetching {title}: {e}")
                writer.writerow(["Error fetching data"])
            f.write("\n")

    written_files.append(overview_file)
    print(f" Saved: {overview_file}")

    # -----------------------------
    # Other Acquisition Reports
    # -----------------------------
    reports = [
        {
            "filename": "Lead acquisition.csv",
            "dimensions": ["firstUserDefaultChannelGroup"],
            "custom_metrics": [
                {"alias": "New leads", "eventName": "generate_lead"},
                {"alias": "Qualified leads", "eventName": "qualify_lead"},
                {"alias": "Converted leads", "eventName": "close_convert_lead"},
            ],
            "metrics": ["userKeyEventRate"],
            "headers": [
                "First user primary channel group",
                "New leads",
                "Qualified leads",
                "Converted leads",
                "User key event rate",
            ],
        },
        {
            "filename": "User acquisition - First user primary channel group.csv",
            "dimensions": ["firstUserDefaultChannelGroup"],
            "metrics": [
                "totalUsers",
                "newUsers",
                "averageSessionDuration",
                "engagedSessions",
                "eventCount",
                "keyEvents",
                "userKeyEventRate",
            ],
            "headers": [
                "First user primary channel group (Default Channel Group)",
                "Total users",
                "New users",
                "Returning users",
                "Average engagement time per active user",
                "Engaged sessions per active user",
                "Event count",
                "Key events",
                "User key event rate",
            ],
        },
        {
            "filename": "AI Traffic - Session.csv",
            "dimensions": ["sessionSource"],
            "metrics": ["newUsers", "sessions", "engagementRate", "averageSessionDuration", "bounceRate", "engagedSessions"],
            "headers": ["Session source", "New users", "Sessions", "Engagement rate", "Average engagement time per session", "Bounce rate", "Engaged sessions"],
        },
        {
            "filename": "Non Google cost.csv",
            "dimensions": ["sessionCampaignId"],
            "metrics": ["activeUsers", "sessions", "engagedSessions", "keyEvents"],
            "headers": ["Session campaign","Active users","Sessions","Engaged sessions","Key events"]
        },
        {
            "filename": "Traffic acquisition - Session primary channel group.csv",
            "dimensions": ["sessionSourceMedium"],
            "metrics": ["newUsers", "sessions", "engagementRate", "averageSessionDuration", "bounceRate", "engagedSessions", "purchaseRevenue"],
            "headers": ["Session source / medium", "New users", "Sessions", "Engagement rate", "Average engagement time per session", "Bounce rate", "Engaged sessions", "Purchase revenue"]
        },
        {
            "filename": "User acquisition cohorts.csv",
            "dimensions": ["firstUserDefaultChannelGroup"],
            "metrics": ["newUsers", "totalRevenue", "transactions", "averagePurchaseRevenuePerUser"],
            "headers": ["First user primary channel group", "New users", "Total revenue", "Transactions", "Average 120d value"],
        },
    ]

    for report in reports:
        print(f"Fetching {report['filename']} ...")
        dimensions_list = [Dimension(name=d) for d in report["dimensions"]]

        if report["filename"] == "User acquisition - First user primary channel group.csv":
            resp = safe_report(client, property_id, report["dimensions"], report["metrics"], start_date, end_date)
            rows = [row_to_values(row) for row in resp.rows]
            df = pd.DataFrame(rows, columns=report["dimensions"] + report["metrics"])
            df = df.apply(pd.to_numeric, errors='ignore').fillna(0)

           
            df["Returning users"] = df["totalUsers"] - df["newUsers"]
            df["Engaged sessions per active user"] = df["engagedSessions"] / df["totalUsers"]
            df["Average engagement time per active user"] = df["averageSessionDuration"] / df["totalUsers"]

            df = df[report["dimensions"] + [
                "totalUsers", "newUsers", "Returning users",
                "Average engagement time per active user",
                "Engaged sessions per active user", "eventCount", "keyEvents", "userKeyEventRate"
            ]]

            file_path = os.path.join(acquisition_dir, report["filename"])
            df.to_csv(file_path, index=False)
            written_files.append(file_path)

        elif "custom_metrics" in report:
            df_list = []
            for m in report["custom_metrics"]:
                request = RunReportRequest(
                    property=f"properties/{property_id}",
                    dimensions=dimensions_list,
                    metrics=[Metric(name="eventCount")],
                    dimension_filter=FilterExpression(
                        filter=Filter(
                            field_name="eventName",
                            string_filter=Filter.StringFilter(value=m["eventName"])
                        )
                    ),
                    date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                )
                resp = client.run_report(request)
                rows = [row_to_values(row) for row in resp.rows]
                df = pd.DataFrame(rows, columns=report["dimensions"] + [m["alias"]])
                df_list.append(df)

            merged_df_custom = df_list[0]
            for df in df_list[1:]:
                merged_df_custom = pd.merge(merged_df_custom, df, on=report["dimensions"], how="outer")

            resp = safe_report(client, property_id, report["dimensions"], report["metrics"], start_date, end_date)
            extra_rows = [row_to_values(row) for row in resp.rows]
            extra_df = pd.DataFrame(extra_rows, columns=report["dimensions"] + report["metrics"])
            merged_df_custom = pd.merge(merged_df_custom, extra_df, on=report["dimensions"], how="outer")

            rows = merged_df_custom.fillna(0).values.tolist()
            file_path = os.path.join(acquisition_dir, report["filename"])
            write_csv(file_path, report["headers"], rows)
            written_files.append(file_path)

        else:
            resp = safe_report(client, property_id, report["dimensions"], report["metrics"], start_date, end_date)
            rows = [row_to_values(row) for row in resp.rows]
            file_path = os.path.join(acquisition_dir, report["filename"])
            write_csv(file_path, report["headers"], rows)
            written_files.append(file_path)

    print("\nAll acquisition reports downloaded successfully.")
    return written_files


def fetch_ga4_engagement_reports(service_account_file, property_id, output_dir, start_date=None, end_date=None):
    

    
    today = date.today()
    weekday = today.weekday()  

    if weekday < 4: 
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = today
    elif weekday == 4:  
        start_date_dt = today - timedelta(days=4)
        end_date_dt = today - timedelta(days=1)
    else:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = start_date_dt + timedelta(days=4)

    start_date = start_date_dt.isoformat()
    end_date = end_date_dt.isoformat()

    print(f"Fetching GA4 Acquisition Reports data from {start_date_dt} to {end_date_dt}")


    
    engagement_dir = os.path.join(output_dir, "Engagement Reports")
    os.makedirs(engagement_dir, exist_ok=True)

    creds = _load_credentials(service_account_file)
    client = BetaAnalyticsDataClient(credentials=creds)
    written_files = []

    
    def safe_report(client, property_id, dimensions, metrics, start_date, end_date):
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=100000,
        )
        return client.run_report(request)

    def row_to_values(row):
        return [v.value for v in list(row.dimension_values) + list(row.metric_values)]

    def write_csv(file_path, headers, rows):
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows if rows else [["No data"]])

    # ------------------------------------------------------------------
    # 1️⃣ Engagement Overview  (multi-table like Acquisition Overview)
    # ------------------------------------------------------------------
    overview_file = os.path.join(engagement_dir, "Engagement Overview.csv")
    with open(overview_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)

        tables = [
            ("Nth day - Active users", ["nthDay"], ["activeUsers"]),
            ("Nth day - New users", ["nthDay"], ["newUsers"]),
            ("Nth day - Engaged sessions", ["nthDay"], ["engagedSessions"]),
            ("Nth day - Average engagement time per user", ["nthDay"], ["averageSessionDuration"]),
            ("Page title and screen class - Views", ["pageTitle"], ["screenPageViews"]),
            ("Event name - Event count", ["eventName"], ["eventCount"]),
            ("Device category - Engaged sessions", ["deviceCategory"], ["engagedSessions"]),
            ("Platform - Active users", ["platform"], ["activeUsers"]),
        ]

        for title, dims, mets in tables:
            f.write(f"Table: {title}\n")
            try:
                resp = safe_report(client, property_id, dims, mets, start_date, end_date)
                if not resp.rows:
                    writer.writerow(["No data"])
                else:
                    rows = [row_to_values(row) for row in resp.rows]
                    df = pd.DataFrame(rows, columns=dims + mets)
                    df.to_csv(f, index=False)
            except Exception as e:
                print(f"⚠ Error fetching {title}: {e}")
                writer.writerow(["Error fetching data"])
            f.write("\n")

    written_files.append(overview_file)
    print(f"Saved: {overview_file}")

    # ------------------------------------------------------------------
    # 2️⃣ Events - Event name.csv
    # ------------------------------------------------------------------
    print(" Fetching Events - Event name.csv ...")
    events_report = {
        "filename": "Events - Event name.csv",
        "dimensions": ["eventName"],
        "metrics": ["eventCount", "totalUsers", "eventCountPerUser", "purchaseRevenue"],
        "headers": ["Event name", "Event count", "Total users", "Event count per active user", "Total revenue"],
    }

    try:
        resp = safe_report(client, property_id, events_report["dimensions"], events_report["metrics"], start_date, end_date)
        rows = [row_to_values(row) for row in resp.rows]
        path = os.path.join(engagement_dir, events_report["filename"])
        write_csv(path, events_report["headers"], rows)
        written_files.append(path)
    except Exception as e:
        print(f"⚠ Error fetching Events report: {e}")

    # ------------------------------------------------------------------
    # 3️⃣ Pages and screens.csv
    # ------------------------------------------------------------------
    print(" Fetching Pages and screens.csv ...")
    pages_report = {
        "filename": "Pages and screens.csv",
        "dimensions": ["pageTitle"],
        "metrics": [
            "newUsers", "sessions", "engagementRate",
            "averageSessionDuration", "screenPageViews",
            "bounceRate", "eventCount", "purchaseRevenue"
        ],
        "headers": [
            "Page title and screen class", "New users", "Sessions", "Engagement rate",
            "Average engagement time per session", "Views", "Bounce rate",
            "Events", "Purchases",
        ],
    }

    try:
        resp = safe_report(client, property_id, pages_report["dimensions"], pages_report["metrics"], start_date, end_date)
        rows = [row_to_values(row) for row in resp.rows]
        path = os.path.join(engagement_dir, pages_report["filename"])
        write_csv(path, pages_report["headers"], rows)
        written_files.append(path)
    except Exception as e:
        print(f"⚠ Error fetching Pages and screens: {e}")

    # ------------------------------------------------------------------
    # 4️⃣ Landing page.csv
    # ------------------------------------------------------------------
    print(" Fetching Landing page.csv ...")
    landing_report = {
        "filename": "Landing page.csv",
        "dimensions": ["landingPagePlusQueryString"],
        "metrics": [
            "activeUsers", "newUsers", "purchaseRevenue",
            "bounceRate", "averageSessionDuration", "sessions"
        ],
        "headers": [
            "Landing page", "Active users", "New users", "Total revenue",
            "Bounce rate", "Average engagement time per session", "Sessions",
        ],
    }

    try:
        resp = safe_report(client, property_id, landing_report["dimensions"], landing_report["metrics"], start_date, end_date)
        rows = [row_to_values(row) for row in resp.rows]
        path = os.path.join(engagement_dir, landing_report["filename"])
        write_csv(path, landing_report["headers"], rows)
        written_files.append(path)
    except Exception as e:
        print(f"⚠ Error fetching Landing page: {e}")

    print("\n All Engagement Reports downloaded successfully!")
    return written_files


def fetch_ga4_monetization_reports(service_account_file, property_id, output_dir, start_date=None, end_date=None):
    

    today = date.today()
    weekday = today.weekday()  

    if weekday < 4:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = today
    elif weekday == 4:  
        start_date_dt = today - timedelta(days=4)
        end_date_dt = today - timedelta(days=1)
    else:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = start_date_dt + timedelta(days=4)

    start_date = start_date_dt.isoformat()
    end_date = end_date_dt.isoformat()

    print(f" Fetching GA4 Acquisition Reports data from {start_date_dt} to {end_date_dt}")

    
    monetization_dir = os.path.join(output_dir, "Monetization Reports")
    os.makedirs(monetization_dir, exist_ok=True)

    creds = _load_credentials(service_account_file)
    client = BetaAnalyticsDataClient(credentials=creds)
    written_files = []

    
    def safe_report(client, property_id, dimensions, metrics, start_date, end_date):
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=100000
        )
        return client.run_report(request)

    def row_to_values(row):
        return [v.value for v in list(row.dimension_values) + list(row.metric_values)]

    def write_csv(file_path, headers, rows):
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows if rows else [["No data"]])

    # -----------------------------------------------------------------
    # 1️⃣ Monetization Overview.csv  (multi-table format)
    # -----------------------------------------------------------------
    overview_file = os.path.join(monetization_dir, "Monetization Overview.csv")
    with open(overview_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)

        tables = [
            ("Nth day - Total revenue", ["nthDay"], ["totalRevenue"]),
            ("Nth day - Purchase revenue", ["nthDay"], ["purchaseRevenue"]),
            ("Nth day - Total ad revenue", ["nthDay"], ["totalAdRevenue"]),
            ("Nth day - Total purchasers", ["nthDay"], ["totalPurchasers"]),
            ("Nth day - First time purchasers", ["nthDay"], ["firstTimePurchasers"]),
            ("Nth day - Average purchase revenue per paying user", ["nthDay"], ["averagePurchaseRevenuePerPayingUser"]),
            ("Item name - Items purchased", ["itemName"], ["itemPurchaseQuantity"]),
            ("Order coupon - Items purchased", ["orderCoupon"], ["itemPurchaseQuantity"]),
            ("Item list name - Items purchased", ["itemListName"], ["itemPurchaseQuantity"]),
        ]

        for title, dims, mets in tables:
            f.write(f"Table: {title}\n")
            try:
                resp = safe_report(client, property_id, dims, mets, start_date, end_date)
                if not resp.rows:
                    writer.writerow(["No data"])
                else:
                    rows = [row_to_values(row) for row in resp.rows]
                    df = pd.DataFrame(rows, columns=dims + mets)
                    df.to_csv(f, index=False)
            except Exception as e:
                print(f"⚠ Error fetching {title}: {e}")
                writer.writerow(["Error fetching data"])
            f.write("\n")

    written_files.append(overview_file)
    print(f" Saved: {overview_file}")

    # -----------------------------------------------------------------
    # 2️⃣ Purchase journey: Device category.csv
    # -----------------------------------------------------------------
    print("Fetching Purchase journey - Device category.csv ...")
    pj = {
        "filename": "Purchase journey - Device category.csv",
        "dimensions": ["deviceCategory"],
        "metrics": ["eventCount", "itemsViewed", "itemsAddedToCart", "checkouts", "ecommercePurchases"],
        "headers": ["Device category", "Total events", "View product events", "Add to cart events", "Begin checkout events", "Purchase events"],
    }
    try:
        resp = safe_report(client, property_id, pj["dimensions"], pj["metrics"], start_date, end_date)
        rows = [row_to_values(row) for row in resp.rows]
        file_path = os.path.join(monetization_dir, pj["filename"])
        write_csv(file_path, pj["headers"], rows)
        written_files.append(file_path)
    except Exception as e:
        print(f"⚠ Error fetching Purchase journey: {e}")

    # -----------------------------------------------------------------
    # 3️⃣ Ecommerce purchases - Item name.csv
    # -----------------------------------------------------------------
    print(" Fetching Ecommerce purchases - Item name.csv ...")
    ecom = {
        "filename": "Ecommerce purchases - Item name.csv",
        "dimensions": ["itemName"],
        "metrics": ["itemsViewed", "itemsAddedToCart", "itemsPurchased", "itemRevenue"],
        "headers": ["Item name", "Items viewed", "Items added to cart", "Items purchased", "Item revenue"],
    }
    try:
        resp = safe_report(client, property_id, ecom["dimensions"], ecom["metrics"], start_date, end_date)
        rows = [row_to_values(row) for row in resp.rows]
        file_path = os.path.join(monetization_dir, ecom["filename"])
        write_csv(file_path, ecom["headers"], rows)
        written_files.append(file_path)
    except Exception as e:
        print(f"⚠ Error fetching Ecommerce purchases: {e}")

    # -----------------------------------------------------------------
    # 4️⃣ Promotions - Item promotion name.csv
    # -----------------------------------------------------------------
    print(" Fetching Promotions - Item promotion name.csv ...")
    promo = {
        "filename": "Promotions - Item promotion name.csv",
        "dimensions": ["itemPromotionName"],
        "metrics": [
            "itemsViewedInPromotion",
            "itemsClickedInPromotion",
            "itemPromotionClickThroughRate",
            "itemsAddedToCart",
            "itemsCheckedOut",
            "itemsPurchased",
            "itemRevenue",
        ],
        "headers": [
            "Item promotion name",
            "Items viewed in promotion",
            "Items clicked in promotion",
            "Item promotion click through rate",
            "Items added to cart",
            "Items checked out",
            "Items purchased",
            "Item revenue",
        ],
    }
    try:
        resp = safe_report(client, property_id, promo["dimensions"], promo["metrics"], start_date, end_date)
        rows = [row_to_values(row) for row in resp.rows]
        file_path = os.path.join(monetization_dir, promo["filename"])
        write_csv(file_path, promo["headers"], rows)
        written_files.append(file_path)
    except Exception as e:
        print(f"⚠ Error fetching Promotions: {e}")

    # -----------------------------------------------------------------
    # 5️⃣ Checkout journey - Device category.csv
    # -----------------------------------------------------------------
    print("Fetching Checkout journey - Device category.csv ...")
    checkout = {
        "filename": "Checkout journey - Device category.csv",
        "dimensions": ["deviceCategory"],
        "metrics": ["checkouts", "shippingAmount", "ecommercePurchases", "purchaseRevenue"],
        "headers": ["Device category", "Begin checkout events", "Shipping amount", "Purchase events", "Purchase revenue"],
    }
    try:
        resp = safe_report(client, property_id, checkout["dimensions"], checkout["metrics"], start_date, end_date)
        rows = [row_to_values(row) for row in resp.rows]
        file_path = os.path.join(monetization_dir, checkout["filename"])
        write_csv(file_path, checkout["headers"], rows)
        written_files.append(file_path)
    except Exception as e:
        print(f"⚠ Error fetching Checkout journey: {e}")

    # -----------------------------------------------------------------
    # 6️⃣ Transactions - Transaction ID.csv
    # -----------------------------------------------------------------
    print(" Fetching Transactions - Transaction ID.csv ...")
    txn = {
        "filename": "Transactions - Transaction ID.csv",
        "dimensions": ["transactionId"],
        "metrics": ["ecommercePurchases", "purchaseRevenue"],
        "headers": ["Transaction ID", "Ecommerce purchases", "Purchase revenue"],
    }
    try:
        resp = safe_report(client, property_id, txn["dimensions"], txn["metrics"], start_date, end_date)
        rows = [row_to_values(row) for row in resp.rows]
        file_path = os.path.join(monetization_dir, txn["filename"])
        write_csv(file_path, txn["headers"], rows)
        written_files.append(file_path)
    except Exception as e:
        print(f"⚠ Error fetching Transactions: {e}")

    print("\n All Monetization Reports downloaded successfully!")
    return written_files



def fetch_ga4_retention_reports(service_account_file, property_id, output_dir, start_date=None, end_date=None):
    
    
    today = date.today()
    weekday = today.weekday()  

    if weekday < 4:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = today
    elif weekday == 4:  
        start_date_dt = today - timedelta(days=4)
        end_date_dt = today - timedelta(days=1)
    else:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = start_date_dt + timedelta(days=4)

    start_date = start_date_dt.isoformat()
    end_date = end_date_dt.isoformat()

    print(f" Fetching GA4 Acquisition Reports data from {start_date_dt} to {end_date_dt}")
    
    retention_dir = os.path.join(output_dir, "Retention Reports")
    os.makedirs(retention_dir, exist_ok=True)

    creds = service_account.Credentials.from_service_account_file(service_account_file)
    client = BetaAnalyticsDataClient(credentials=creds)
    written_files = []

    
    def run_cohort_report(dimensions, metrics, start_date, end_date):
        cohort_spec = CohortSpec(
            cohorts=[
                Cohort(
                    name="Retention_Cohort",
                    dimension="firstSessionDate",
                    date_range={"start_date": start_date, "end_date": end_date},
                )
            ],
            cohorts_range=CohortsRange(
                start_offset=0,
                end_offset=7,
                granularity="DAILY",
            ),
        )

        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            cohort_spec=cohort_spec,
            limit=100000,
        )
        return client.run_report(request)

    def row_to_values(row):
        return [v.value for v in list(row.dimension_values) + list(row.metric_values)]

    # -----------------------------------------------------------------
    # 1️⃣ Retention Overview.csv (multi-table format)
    # -----------------------------------------------------------------
    print(" Fetching Retention Overview tables...")
    overview_file = os.path.join(retention_dir, "Retention Overview.csv")
    with open(overview_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)

        tables = [
            ("Cohort - Active users", ["cohort", "cohortNthDay"], ["activeUsers"]),
            ("Cohort - New users", ["cohort", "cohortNthDay"], ["newUsers"]),
            ("Cohort - Engaged sessions", ["cohort", "cohortNthDay"], ["engagedSessions"]),
            ("Cohort - Average engagement time per user", ["cohort", "cohortNthDay"], ["averageSessionDuration"]),
            ("Cohort - User engagement duration", ["cohort", "cohortNthDay"], ["userEngagementDuration"]),
        ]

        for title, dims, mets in tables:
            print(f"   ⏳ Fetching {title} ...")
            f.write(f"Table: {title}\n")
            try:
                resp = run_cohort_report(dims, mets, start_date, end_date)
                if not resp.rows:
                    writer.writerow(["No data"])
                else:
                    rows = [row_to_values(row) for row in resp.rows]
                    df = pd.DataFrame(rows, columns=dims + mets)
                    df.to_csv(f, index=False)
                    print(f"    Saved table: {title}")
            except Exception as e:
                print(f"⚠ Error fetching {title}: {e}")
                writer.writerow(["Error fetching data"])
            f.write("\n")

    written_files.append(overview_file)
    print(f" Saved: {overview_file}")

    print("\n All Retention Reports downloaded successfully!")
    return written_files



def fetch_ga4_users_full(service_account_file, property_id, output_dir, start_date=None, end_date=None):

    today = date.today()
    weekday = today.weekday()  

    if weekday < 4:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = today
    elif weekday == 4:  
        start_date_dt = today - timedelta(days=4)
        end_date_dt = today - timedelta(days=1)
    else:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = start_date_dt + timedelta(days=4)

    start_date = start_date_dt.isoformat()
    end_date = end_date_dt.isoformat()

    print(f" Fetching GA4 Acquisition Reports data from {start_date_dt} to {end_date_dt}")
    user_attr_dir = os.path.join(output_dir, "User Attributes Reports")
    tech_dir = os.path.join(output_dir, "Tech Reports")
    os.makedirs(user_attr_dir, exist_ok=True)
    os.makedirs(tech_dir, exist_ok=True)

    creds = service_account.Credentials.from_service_account_file(service_account_file)
    client = BetaAnalyticsDataClient(credentials=creds)
    saved_files = []
    
    def safe_report(dimensions, metrics):
        try:
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                limit=100000
            )
            response = client.run_report(request)
            records = []
            for row in response.rows:
                rec = {h.name: v.value for h, v in zip(response.dimension_headers, row.dimension_values)}
                rec.update({h.name: v.value for h, v in zip(response.metric_headers, row.metric_values)})
                records.append(rec)
            return pd.DataFrame(records)
        except Exception as e:
            print(f"⚠ Error fetching {dimensions} with {metrics}: {e}")
            return pd.DataFrame()

    def write_csv(df, path, headers=None):
        if df.empty:
            df = pd.DataFrame([["No data"]], columns=["Message"])
        elif headers:
            df.columns = headers
        df.to_csv(path, index=False)
        print(f" Saved: {path}")
        saved_files.append(path)

    # -----------------------------------------------------------------
    # 1️⃣ User Attributes Reports
    # -----------------------------------------------------------------
    print("📊 Generating User Attributes Reports...")

    # 1. Demographic details
    df_country = safe_report(
        ["country"],
        ["activeUsers", "newUsers", "engagedSessions", "engagementRate",
         "averageSessionDuration", "eventCount", "totalRevenue"]
    )
    path_country = os.path.join(user_attr_dir, "Demographic details - Country.csv")
    write_csv(df_country, path_country)

    # 2. Audiences
    df_audience = safe_report(
        ["audienceName"],
        ["totalUsers", "newUsers", "sessions", "screenPageViewsPerSession",
         "averageSessionDuration", "totalRevenue"]
    )
    path_audience = os.path.join(user_attr_dir, "Audiences - Audience name.csv")
    write_csv(df_audience, path_audience)

    # 3. User Attributes Overview (multi-table)
    print(" Generating User Attributes Overview...")
    overview_user_attr = os.path.join(user_attr_dir, "User attributes Overview.csv")
    with open(overview_user_attr, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        tables = [
            ("Country ID", ["countryId"], ["activeUsers"]),
            ("City", ["city"], ["activeUsers"]),
            ("Language", ["language"], ["activeUsers"]),
            ("Region", ["region"], ["activeUsers"]),
            ("Continent", ["continent"], ["activeUsers"]),
        ]
        for title, dims, mets in tables:
            f.write(f"Table: {title}\n")
            df = safe_report(dims, mets)
            if df.empty:
                writer.writerow(["No data"])
            else:
                df.to_csv(f, index=False)
            f.write("\n")
    saved_files.append(overview_user_attr)
    print(f" Saved: {overview_user_attr}")

    # -----------------------------------------------------------------
    # 2️⃣ Tech Reports
    # -----------------------------------------------------------------
    print(" Generating Tech Reports...")

    
    df_tech = safe_report(
        ["browser"],
        ["activeUsers", "newUsers", "engagedSessions", "engagementRate",
         "averageSessionDuration", "eventCount", "totalRevenue"]
    )
    path_tech = os.path.join(tech_dir, "Tech details - Browser.csv")
    write_csv(df_tech, path_tech)

    
    print(" Generating Tech Overview...")
    overview_tech = os.path.join(tech_dir, "Tech Overview.csv")
    with open(overview_tech, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        tables = [
            ("Platform", ["platform"], ["activeUsers"]),
            ("Operating system", ["operatingSystem"], ["activeUsers"]),
            ("Platform / device category", ["platformDeviceCategory"], ["activeUsers"]),
            ("Browser", ["browser"], ["activeUsers"]),
            ("Device category", ["deviceCategory"], ["activeUsers"]),
            ("Screen resolution", ["screenResolution"], ["activeUsers"]),
        ]
        for title, dims, mets in tables:
            f.write(f"Table: {title}\n")
            df = safe_report(dims, mets)
            if df.empty:
                writer.writerow(["No data"])
            else:
                df.to_csv(f, index=False)
            f.write("\n")
    saved_files.append(overview_tech)
    print(f" Saved: {overview_tech}")

    print("\n All GA4 User Attribute & Tech Reports generated successfully!")
    return saved_files

def fetch_generate_leads_full(service_account_file, property_id, output_dir, start_date=None, end_date=None):

    today = date.today()
    weekday = today.weekday()  

    if weekday < 4:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = today
    elif weekday == 4:  
        start_date_dt = today - timedelta(days=4)
        end_date_dt = today - timedelta(days=1)
    else:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = start_date_dt + timedelta(days=4)
    start_date = start_date_dt.isoformat()
    end_date = end_date_dt.isoformat()
    print(f" Fetching GA4 Acquisition Reports data from {start_date_dt} to {end_date_dt}")
    gen_dir = os.path.join(output_dir, "Generate Leads Reports")
    os.makedirs(gen_dir, exist_ok=True)
    saved_files = []
    creds = service_account.Credentials.from_service_account_file(service_account_file)
    client = BetaAnalyticsDataClient(credentials=creds)
    def safe_report(dimensions, metrics, dimension_filter=None):
        try:
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimension_filter=dimension_filter,
                limit=100000,
            )
            response = client.run_report(request)
            records = []
            for row in response.rows:
                rec = {h.name: v.value for h, v in zip(response.dimension_headers, row.dimension_values)}
                rec.update({h.name: v.value for h, v in zip(response.metric_headers, row.metric_values)})
                records.append(rec)
            return pd.DataFrame(records)
        except Exception as e:
            print(f"⚠ Error fetching {dimensions} with {metrics}: {e}")
            return pd.DataFrame()

    def write_csv(df, filename, headers=None):
        path = os.path.join(gen_dir, filename)
        if df.empty:
            df = pd.DataFrame([["No data"]], columns=["Message"])
        elif headers:
            df.columns = headers
        df.to_csv(path, index=False)
        print(f" Saved: {path}")
        saved_files.append(path)

    # -----------------------------------------------------------------
    # 1️⃣ Traffic acquisition
    # -----------------------------------------------------------------
    df_traffic = safe_report(
        ["sessionSourceMedium"],
        ["newUsers", "sessions", "engagementRate", "averageSessionDuration", "bounceRate", "engagedSessions", "purchaseRevenue"],
    )
    write_csv(df_traffic, "Traffic acquisition.csv")

    # -----------------------------------------------------------------
    # 2️⃣ User acquisition cohorts
    # -----------------------------------------------------------------
    df_user_cohort = safe_report(
        ["firstUserDefaultChannelGroup"],
        ["newUsers", "totalRevenue", "transactions", "averagePurchaseRevenuePerUser"],
    )
    write_csv(df_user_cohort, "User acquisition cohorts.csv")

    # -----------------------------------------------------------------
    # 3️⃣ Lead acquisition (generate, qualify, convert)
    # -----------------------------------------------------------------
    print(" Fetching Lead acquisition events...")
    dim = "firstUserDefaultChannelGroup"

    def event_count(event_name, alias):
        filter_exp = FilterExpression(
            filter=Filter(field_name="eventName", string_filter=Filter.StringFilter(value=event_name))
        )
        df = safe_report([dim], ["eventCount"], filter_exp)
        if not df.empty:
            df = df.rename(columns={"eventCount": alias})
        return df

    df_gen = event_count("generate_lead", "New leads")
    df_qual = event_count("qualify_lead", "Qualified leads")
    df_conv = event_count("close_convert_lead", "Converted leads")
    df_total = safe_report([dim], ["totalUsers"])

    df_leads = df_total.copy()
    for sub_df in [df_gen, df_qual, df_conv]:
        if not sub_df.empty:
            df_leads = df_leads.merge(sub_df, on=dim, how="outer")

    for col in ["New leads", "Qualified leads", "Converted leads", "totalUsers"]:
        if col not in df_leads.columns:
            df_leads[col] = 0

    df_leads["User key event rate"] = (
        pd.to_numeric(df_leads["New leads"], errors="coerce").fillna(0)
        + pd.to_numeric(df_leads["Qualified leads"], errors="coerce").fillna(0)
        + pd.to_numeric(df_leads["Converted leads"], errors="coerce").fillna(0)
    ) / pd.to_numeric(df_leads["totalUsers"], errors="coerce").replace({0: pd.NA})

    df_leads["User key event rate"] = df_leads["User key event rate"].fillna(0)

    write_csv(df_leads, "Lead acquisition.csv")

    # -----------------------------------------------------------------
    # 4️⃣ Lead disqualification
    # -----------------------------------------------------------------
    df_loss = safe_report(["eventName"], ["eventCount", "totalUsers"])
    if not df_loss.empty:
        df_loss["Event count per active user"] = pd.to_numeric(df_loss["eventCount"], errors="coerce") / \
                                                pd.to_numeric(df_loss["totalUsers"], errors="coerce").replace({0: pd.NA})
        df_loss["Event count per active user"] = df_loss["Event count per active user"].fillna(0)
    write_csv(df_loss, "Lead disqualification.csv")

    # -----------------------------------------------------------------
    # 5️⃣ Audiences
    # -----------------------------------------------------------------
    df_audience = safe_report(
        ["audienceName"],
        ["totalUsers", "newUsers", "sessions", "screenPageViewsPerSession", "averageSessionDuration", "totalRevenue"],
    )
    write_csv(df_audience, "Audiences.csv")

    # -----------------------------------------------------------------
    # 6️⃣ Landing page
    # -----------------------------------------------------------------
    df_lp = safe_report(
        ["landingPage"],
        ["activeUsers", "newUsers", "totalRevenue", "bounceRate", "averageSessionDuration", "sessions"],
    )
    write_csv(df_lp, "Landing page.csv")

    # -----------------------------------------------------------------
    # 7️⃣ User acquisition
    # -----------------------------------------------------------------
    df_user_acq = safe_report(
        ["firstUserDefaultChannelGroup"],
        ["totalUsers", "newUsers", "averageSessionDuration", "engagedSessions", "eventCount"],
    )
    if not df_user_acq.empty:
        df_user_acq["Returning users"] = (
            pd.to_numeric(df_user_acq["totalUsers"], errors="coerce").fillna(0)
            - pd.to_numeric(df_user_acq["newUsers"], errors="coerce").fillna(0)
        )
        df_user_acq["Engaged sessions per active user"] = (
            pd.to_numeric(df_user_acq["engagedSessions"], errors="coerce").fillna(0)
            / pd.to_numeric(df_user_acq["totalUsers"], errors="coerce").replace({0: pd.NA})
        ).fillna(0)
        df_user_acq["User key event rate"] = (
            pd.to_numeric(df_user_acq["eventCount"], errors="coerce")
            / pd.to_numeric(df_user_acq["totalUsers"], errors="coerce").replace({0: pd.NA})
        ).fillna(0)
    write_csv(df_user_acq, "User acquisition.csv")

    # -----------------------------------------------------------------
    # 8️⃣ Generate Leads Overview (multi-table + funnel summary)
    # -----------------------------------------------------------------
    overview_path = os.path.join(gen_dir, "Generate Leads Overview.csv")
    with open(overview_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)

        tables = [
            ("Nth day - New users", ["nthDay"], ["newUsers"]),
            ("Nth day - Returning users", ["nthDay"], ["totalUsers", "newUsers"]),
            ("Platform - Key events", ["platform"], ["eventCount"]),
            ("First user primary channel group - New users", ["firstUserDefaultChannelGroup"], ["newUsers"]),
            ("Audience name - Active users", ["audienceName"], ["activeUsers"]),
            ("City - Active users", ["city"], ["activeUsers"]),
        ]

        for title, dims, mets in tables:
            f.write(f"Table: {title}\n")
            df = safe_report(dims, mets)
            if title == "Nth day - Returning users" and not df.empty:
                df["Returning users"] = (
                    pd.to_numeric(df.get("totalUsers", 0), errors="coerce").fillna(0)
                    - pd.to_numeric(df.get("newUsers", 0), errors="coerce").fillna(0)
                )
                df = df[["nthDay", "Returning users"]]
            if df.empty:
                writer.writerow(["No data"])
            else:
                df.to_csv(f, index=False)
            f.write("\n")

        
        f.write("Table: Lead Funnel Summary\n")
        if not df_leads.empty:
            total_new = df_leads["New leads"].sum()
            total_qual = df_leads["Qualified leads"].sum()
            total_conv = df_leads["Converted leads"].sum()

            if total_new > 0:
                qual_rate = (total_qual / total_new) * 100
            else:
                qual_rate = 0

            if total_qual > 0:
                conv_rate = (total_conv / total_qual) * 100
            else:
                conv_rate = 0

            summary = pd.DataFrame(
                [
                    ["Total New Leads", total_new],
                    ["Total Qualified Leads", total_qual],
                    ["Total Converted Leads", total_conv],
                    ["Qualification Rate (%)", round(qual_rate, 2)],
                    ["Conversion Rate (%)", round(conv_rate, 2)],
                ],
                columns=["Metric", "Value"],
            )
            summary.to_csv(f, index=False)
        else:
            writer.writerow(["No data"])
        f.write("\n")

    saved_files.append(overview_path)
    print(f" Saved: {overview_path}")

    print("\n All 'Generate Leads' Reports generated successfully!")
    return saved_files

def fetch_drive_sales_full(service_account_file, property_id, output_dir):
    
    today = date.today()
    weekday = today.weekday()  

    if weekday < 4:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = today
    elif weekday == 4:  
        start_date_dt = today - timedelta(days=4)
        end_date_dt = today - timedelta(days=1)
    else:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = start_date_dt + timedelta(days=4)

    start_date = start_date_dt.isoformat()
    end_date = end_date_dt.isoformat()

    print(f" Fetching GA4 Acquisition Reports data from {start_date_dt} to {end_date_dt}")

    print(" Fetching 'Drive Sales' reports...")

   
    credentials = service_account.Credentials.from_service_account_file(service_account_file)
    client = BetaAnalyticsDataClient(credentials=credentials)

    
    drive_dir = os.path.join(output_dir, "Drive Sales Reports")
    os.makedirs(drive_dir, exist_ok=True)
    saved_files = []

    
    def run_report_to_df(dimensions, metrics):
        try:
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
                limit=100000
            )
            response = client.run_report(request)
            rows = []
            for row in response.rows:
                rec = {h.name: v.value for h, v in zip(response.dimension_headers, row.dimension_values)}
                rec.update({h.name: v.value for h, v in zip(response.metric_headers, row.metric_values)})
                rows.append(rec)
            return pd.DataFrame(rows)
        except Exception as e:
            print(f"⚠ Error fetching {dimensions} / {metrics}: {e}")
            return pd.DataFrame()

    def save_csv(df, path, rename_cols):
        if df.empty:
            pd.DataFrame([["No data"]], columns=["Message"]).to_csv(path, index=False)
            print(f"⚠ Saved (no data): {path}")
        else:
            df = df.rename(columns=rename_cols)
            df.to_csv(path, index=False)
            print(f" Saved: {path}")
        saved_files.append(path)

    # --- 1️⃣ Purchase journey ---
    df_pj = run_report_to_df(["deviceCategory"], ["sessions", "itemViews", "addToCarts", "checkouts", "ecommercePurchases"])
    save_csv(df_pj, os.path.join(drive_dir, "Purchase journey - Device category.csv"), {
        "deviceCategory": "Device category",
        "sessions": "Session start Active users",
        "itemViews": "View product Active users",
        "addToCarts": "Add to cart Active users",
        "checkouts": "Begin checkout Active users",
        "ecommercePurchases": "Purchase Active users"
    })

    # --- 2️⃣ Promotions ---
    df_prom = run_report_to_df(["itemPromotionName"], ["itemsViewedInPromotion", "itemsClickedInPromotion",
                                                       "itemPromotionClickThroughRate", "itemsAddedToCart",
                                                       "itemsCheckedOut", "itemsPurchased", "itemRevenue"])
    save_csv(df_prom, os.path.join(drive_dir, "Promotions - Item promotion name.csv"), {
        "itemPromotionName": "Item promotion name",
        "itemsViewedInPromotion": "Items viewed in promotion",
        "itemsClickedInPromotion": "Items clicked in promotion",
        "itemPromotionClickThroughRate": "Item promotion click through rate",
        "itemsAddedToCart": "Items added to cart",
        "itemsCheckedOut": "Items checked out",
        "itemsPurchased": "Items purchased",
        "itemRevenue": "Item revenue"
    })

    # --- 3️⃣ Ecommerce purchases ---
    df_ecom = run_report_to_df(["itemName"], ["itemsViewed", "itemsAddedToCart", "itemsPurchased", "itemRevenue"])
    save_csv(df_ecom, os.path.join(drive_dir, "Ecommerce purchases - Item name.csv"), {
        "itemName": "Item name",
        "itemsViewed": "Items viewed",
        "itemsAddedToCart": "Items added to cart",
        "itemsPurchased": "Items purchased",
        "itemRevenue": "Item revenue"
    })

    # --- 4️⃣ Checkout journey ---
    df_checkout = run_report_to_df(["deviceCategory"], ["checkouts", "shippingAmount", "ecommercePurchases"])

    save_csv(df_checkout, os.path.join(drive_dir, "Checkout journey - Device category.csv"), {
    "deviceCategory": "Device category",
    "checkouts": "Begin checkout Active users",
    "shippingAmount": "Add shipping Active users",
    "ecommercePurchases": "Purchase Active users"
})


    # --- 5️⃣ Transactions ---
    df_txn = run_report_to_df(["transactionId"], ["ecommercePurchases", "purchaseRevenue"])
    save_csv(df_txn, os.path.join(drive_dir, "Transactions - Transaction ID.csv"), {
        "transactionId": "Transaction ID",
        "ecommercePurchases": "Ecommerce purchases",
        "purchaseRevenue": "Purchase revenue"
    })

    # --- 6️⃣ Drive Sales Overview ---
    overview_path = os.path.join(drive_dir, "Drive Sales Overview.csv")
    with open(overview_path, "w", encoding="utf-8", newline="") as f:
        tables = [
            ("Nth day - Total revenue", ["nthDay"], ["totalRevenue"]),
            ("Nth day - Ecommerce revenue", ["nthDay"], ["purchaseRevenue"]),
            ("Nth day - Total purchasers", ["nthDay"], ["totalPurchasers"]),
            ("Nth day - First time purchasers", ["nthDay"], ["firstTimePurchasers"]),
            ("Nth day - Average purchase revenue per active user", ["nthDay"], ["averagePurchaseRevenuePerPayingUser"]),
            ("Item name - Items purchased", ["itemName"], ["itemsPurchased"]),
            ("Order coupon - Items purchased", ["orderCoupon"], ["itemsPurchased"]),
            ("Item list name - Items purchased", ["itemListName"], ["itemsPurchased"]),
        ]
        writer = csv.writer(f)
        for title, dims, mets in tables:
            f.write(f"Table: {title}\n")
            df = run_report_to_df(dims, mets)
            if df.empty:
                writer.writerow(["No data"])
            else:
                df.to_csv(f, index=False)
            f.write("\n")

    saved_files.append(overview_path)
    print(f" Saved: {overview_path}")

    print("All 'Drive Sales' reports generated successfully!")
    return saved_files




def fetch_understand_web_full(service_account_file, property_id, output_dir):
    """Fetch GA4 'Understand Web' reports and save all CSV files under 'Understand Web Reports' folder."""

    
    today = date.today()
    weekday = today.weekday()  

    if weekday < 4:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = today
    elif weekday == 4:  
        start_date_dt = today - timedelta(days=4)
        end_date_dt = today - timedelta(days=1)
    else:  
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = start_date_dt + timedelta(days=4)

    start_date = start_date_dt.isoformat()
    end_date = end_date_dt.isoformat()

    print(f" Fetching GA4 Acquisition Reports data from {start_date_dt} to {end_date_dt}")

    print(" Fetching 'Understand Web' reports...")

    
    credentials = service_account.Credentials.from_service_account_file(service_account_file)
    client = BetaAnalyticsDataClient(credentials=credentials)

    uw_dir = os.path.join(output_dir, "Understand Web Reports")
    os.makedirs(uw_dir, exist_ok=True)
    saved_files = []

    def run_report_with_pagination(request):
        all_rows = []
        response = client.run_report(request)
        all_rows.extend(response.rows)
        total_rows = len(response.rows)
        while response.row_count > total_rows:
            if not response.metadata.next_page_token:
                break
            request.page_token = response.metadata.next_page_token
            response = client.run_report(request)
            all_rows.extend(response.rows)
            total_rows += len(response.rows)
        return response, all_rows

    # --- ⚙️ Helper: Run GA4 Query ---
    def run_report_to_df(dimensions, metrics):
        try:
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
                limit=100000
            )
            response, rows = run_report_with_pagination(request)
            records = []
            for row in rows:
                rec = {h.name: v.value for h, v in zip(response.dimension_headers, row.dimension_values)}
                rec.update({h.name: v.value for h, v in zip(response.metric_headers, row.metric_values)})
                records.append(rec)
            return pd.DataFrame(records)
        except Exception as e:
            print(f"⚠ Error fetching dims={dimensions}, metrics={metrics} — {e}")
            return pd.DataFrame()

    # --- ⚙️ Helper: Save CSV ---
    def save_csv(df, path, columns):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if df.empty:
            df = pd.DataFrame([["No data"]], columns=["Message"])
            df.to_csv(path, index=False)
            print(f"⚠ Saved (no data): {path}")
        else:
            df = df.rename(columns=columns)
            df.to_csv(path, index=False)
            print(f" Saved: {path} ({len(df)} rows)")
        saved_files.append(path)

    # --- 1️⃣ Demographic details: Country.csv ---
    df_demo = run_report_to_df(
        ["country"],
        ["activeUsers", "newUsers", "engagedSessions", "engagementRate",
         "userEngagementDuration", "eventCount", "purchaseRevenue"]
    )
    rename_demo = {
        "country": "Country",
        "activeUsers": "Active users",
        "newUsers": "New users",
        "engagedSessions": "Engaged sessions",
        "engagementRate": "Engagement rate",
        "userEngagementDuration": "Average engagement time per active user",
        "eventCount": "Event count",
        "purchaseRevenue": "Total revenue"
    }
    save_csv(df_demo, os.path.join(uw_dir, "Demographic details - Country.csv"), rename_demo)

    # --- 2️⃣ Pages and screens: Page title and screen class.csv ---
    df_pages = run_report_to_df(
        ["pageTitle"],
        ["newUsers", "sessions", "engagementRate", "averageSessionDuration",
         "screenPageViews", "bounceRate", "eventsPerSession", "ecommercePurchases"]
    )
    rename_pages = {
        "pageTitle": "Page title and screen class",
        "newUsers": "New users",
        "sessions": "Sessions",
        "engagementRate": "Engagement rate",
        "averageSessionDuration": "Average engagement time per session",
        "screenPageViews": "Views",
        "bounceRate": "Bounce rate",
        "eventsPerSession": "Events per session",
        "ecommercePurchases": "Purchases"
    }
    save_csv(df_pages, os.path.join(uw_dir, "Pages and screens - Page title and screen class.csv"), rename_pages)

    # --- 3️⃣ Understand Web Overview.csv ---
    overview_path = os.path.join(uw_dir, "Understand Web Overview.csv")
    with open(overview_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        tables = [
            ("Country ID - Active users", ["countryId"], ["activeUsers"]),
            ("Country - Active users", ["country"], ["activeUsers"]),
            ("City - Active users", ["city"], ["activeUsers"]),
            ("Nth day - Average engagement time per active user", ["nthDay"], ["userEngagementDuration"]),
            ("Nth day - Average purchase revenue per active user", ["nthDay"], ["purchaseRevenue"]),
            ("Nth day - Engaged sessions per active user", ["nthDay"], ["engagedSessions"]),
            ("Nth day - Average engagement time per session", ["nthDay"], ["averageSessionDuration"]),
            ("Event name - Event count", ["eventName"], ["eventCount"]),
            ("Page title and screen class - Views", ["pageTitle"], ["screenPageViews"]),
            ("Nth day - DAU / MAU, DAU / WAU, WAU / MAU", ["nthDay"], ["dauPerMau", "dauPerWau", "wauPerMau"]),
            ("Language - Active users", ["language"], ["activeUsers"]),
        ]

        for title, dims, mets in tables:
            f.write(f"Table: {title}\n")
            df = run_report_to_df(dims, mets)
            if df.empty:
                writer.writerow(["No data"])
            else:
                df.to_csv(f, index=False)
            f.write("\n")

    saved_files.append(overview_path)
    print(f" Saved: {overview_path}")

    print(" All 'Understand Web' reports generated successfully!")
    return saved_files



def fetch_view_user_engagements_full(service_account_file, property_id, output_dir):
    """Fetch GA4 'View User Engagements' reports and save under 'View User Engagements Reports'."""

    
    today = date.today()
    weekday = today.weekday()  

    if weekday < 4: 
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = today
    elif weekday == 4:  
        start_date_dt = today - timedelta(days=4)
        end_date_dt = today - timedelta(days=1)
    else: 
        start_date_dt = today - timedelta(days=weekday)
        end_date_dt = start_date_dt + timedelta(days=4)

    start_date = start_date_dt.isoformat()
    end_date = end_date_dt.isoformat()

    print(f" Fetching GA4 Acquisition Reports data from {start_date_dt} to {end_date_dt}")

    print(" Fetching 'View User Engagements' reports...")


    credentials = service_account.Credentials.from_service_account_file(service_account_file)
    client = BetaAnalyticsDataClient(credentials=credentials)

    vue_dir = os.path.join(output_dir, "View User Engagements Reports")
    os.makedirs(vue_dir, exist_ok=True)
    saved_files = []

    # --- ⚙️ Helper: Pagination ---
    def run_report_with_pagination(request):
        all_rows = []
        response = client.run_report(request)
        all_rows.extend(response.rows)
        total_rows = len(response.rows)
        while response.row_count > total_rows:
            if not response.metadata.next_page_token:
                break
            request.page_token = response.metadata.next_page_token
            response = client.run_report(request)
            all_rows.extend(response.rows)
            total_rows += len(response.rows)
        return response, all_rows

    # --- ⚙️ Helper: Run GA4 Query ---
    def run_report_to_df(dimensions, metrics):
        try:
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
                limit=100000
            )
            response, rows = run_report_with_pagination(request)
            records = []
            for row in rows:
                rec = {h.name: v.value for h, v in zip(response.dimension_headers, row.dimension_values)}
                rec.update({h.name: v.value for h, v in zip(response.metric_headers, row.metric_values)})
                records.append(rec)
            return pd.DataFrame(records)
        except Exception as e:
            print(f"⚠ Error fetching dims={dimensions}, metrics={metrics} — {e}")
            return pd.DataFrame()

    # --- ⚙️ Helper: Fetch All Metrics Safely ---
    def fetch_all_metrics(dimensions, metrics):
        print(f" Fetching metrics for {dimensions}")
        df_full = pd.DataFrame()
        try:
            df_full = run_report_to_df(dimensions, metrics)
            if not df_full.empty:
                return df_full
            else:
                raise Exception("Empty response, retrying one-by-one")
        except Exception as e:
            print(f"⚠ Retrying one-by-one: {e}")
            dfs = []
            for m in metrics:
                df_m = run_report_to_df(dimensions, [m])
                dfs.append(df_m if not df_m.empty else pd.DataFrame(columns=dimensions + [m]))
            if dfs:
                df_full = dfs[0]
                for df_m in dfs[1:]:
                    df_full = pd.merge(df_full, df_m, on=dimensions[0], how="outer")
        return df_full

    
    def save_csv(df, path, columns):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if df.empty:
            df = pd.DataFrame([["No data"]], columns=["Message"])
            df.to_csv(path, index=False)
            print(f"⚠ Saved (no data): {path}")
        else:
            df = df.rename(columns=columns)
            df.to_csv(path, index=False)
            print(f" Saved: {path} ({len(df)} rows)")
        saved_files.append(path)

    # --- 1️⃣ Pages and Screens: Page title and screen class.csv ---
    df_pages = fetch_all_metrics(
        ["pageTitle"],
        ["newUsers", "sessions", "engagementRate", "averageSessionDuration",
         "screenPageViews", "bounceRate", "eventsPerSession", "ecommercePurchases"]
    )
    rename_pages = {
        "pageTitle": "Page title and screen class",
        "newUsers": "New users",
        "sessions": "Sessions",
        "engagementRate": "Engagement rate",
        "averageSessionDuration": "Average engagement time per session",
        "screenPageViews": "Views",
        "bounceRate": "Bounce rate",
        "eventsPerSession": "Events per session",
        "ecommercePurchases": "Purchases"
    }
    save_csv(df_pages, os.path.join(vue_dir, "Pages and screens - Page title and screen class.csv"), rename_pages)

    # --- 2️⃣ Events: Event name.csv ---
    df_events = fetch_all_metrics(["eventName"], ["eventCount", "totalUsers", "purchaseRevenue"])
    if not df_events.empty and "eventCount" in df_events.columns and "totalUsers" in df_events.columns:
        df_events["Event count per active user"] = (
            pd.to_numeric(df_events["eventCount"], errors="coerce") /
            pd.to_numeric(df_events["totalUsers"], errors="coerce")
        ).fillna(0)
    rename_events = {
        "eventName": "Event name",
        "eventCount": "Event count",
        "totalUsers": "Total users",
        "Event count per active user": "Event count per active user",
        "purchaseRevenue": "Total revenue"
    }
    save_csv(df_events, os.path.join(vue_dir, "Events - Event name.csv"), rename_events)

    # --- 3️⃣ View User Engagements Overview.csv ---
    overview_path = os.path.join(vue_dir, "View User Engagements Overview.csv")
    with open(overview_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        tables = [
            ("Nth day - Active users", ["nthDay"], ["activeUsers"]),
            ("Nth day - New users", ["nthDay"], ["newUsers"]),
            ("First user primary channel group - New users", ["firstUserDefaultChannelGroup"], ["newUsers"]),
            ("Page title and screen class - Views", ["pageTitle"], ["screenPageViews"]),
            ("Platform - Active users", ["platform"], ["activeUsers"])
        ]

        for title, dims, mets in tables:
            f.write(f"Table: {title}\n")
            df = fetch_all_metrics(dims, mets)
            if df.empty:
                writer.writerow(["No data"])
            else:
                df.to_csv(f, index=False)
            f.write("\n")

    saved_files.append(overview_path)
    print(f" Saved: {overview_path}")

    print(" All 'View User Engagements' reports generated successfully!")
    return saved_files


def fetch_ga4_full(service_account_file, property_id, output_dir, start_date=None, end_date=None):
    print(" Starting GA4 full report fetch...")

    os.makedirs(output_dir, exist_ok=True)
    

    # 
    acquisition_files = fetch_ga4_acquisition_reports( 
        service_account_file=service_account_file,
        property_id=property_id,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date
    )
    # 
    engagement_files = fetch_ga4_engagement_reports(
        service_account_file=service_account_file,
        property_id=property_id,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date
    )
    # 
    monetization_files = fetch_ga4_monetization_reports(
        service_account_file=service_account_file,
        property_id=property_id,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date
    )
    # 
    retention_file = fetch_ga4_retention_reports(
        service_account_file=service_account_file,
        property_id=property_id,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date
    )
    # 
    users_files = fetch_ga4_users_full(
        service_account_file=service_account_file,  
        property_id=property_id,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date
    )
    # 
    generate_leads_files = fetch_generate_leads_full(
        service_account_file=service_account_file,
        property_id=property_id,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date
    )
    # 
    drive_sales_files = fetch_drive_sales_full(
        service_account_file=service_account_file,
        property_id=property_id,
        output_dir=output_dir
    )
    # 
    UnderstandWeb_files = fetch_understand_web_full(
        service_account_file=service_account_file,
        property_id=property_id,
        output_dir=output_dir
    )
    # 
    view_user_engagements_files = fetch_view_user_engagements_full(
        service_account_file=service_account_file,
        property_id=property_id,
        output_dir=output_dir
    )



    print(" All GA4 reports fetched successfully!")
    return  acquisition_files + engagement_files +  monetization_files + retention_file + users_files + generate_leads_files + drive_sales_files + UnderstandWeb_files + view_user_engagements_files











