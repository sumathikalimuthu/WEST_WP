# combined_preprocessing_aggregation.py
import os
import glob
import pandas as pd

# -------------------------
# Column mapping (dynamic)
# -------------------------
COLUMN_MAP = {
    "page": ["page", "landing page", "url", "pagepath", "page_location"],
    "clicks": ["clicks", "total clicks", "totalclicks"],
    "impressions": ["impressions", "total impressions", "totalimpressions"],
    "ctr": ["ctr", "click through rate", "clickthroughrate"],
    "position": ["position", "avg position", "avgposition"]
}

# -------------------------
# Normalize columns
# -------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower() for c in df.columns]
    for canonical, variants in COLUMN_MAP.items():
        for v in variants:
            if v in df.columns:
                df.rename(columns={v: canonical}, inplace=True)
                break
    for col in ["clicks", "impressions", "ctr", "position"]:
        if col not in df.columns:
            df[col] = 0
    for col in ["clicks", "impressions", "ctr", "position"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

# -------------------------
# Detect SEO errors
# -------------------------
def detect_seo_errors(df: pd.DataFrame) -> pd.DataFrame:
    df["errors"] = ""
    if "verdict" in df.columns:
        df.loc[df["verdict"] != "PASS", "errors"] += "Indexing issue | "
    if "http_status" in df.columns:
        df.loc[df["http_status"] >= 400, "errors"] += "HTTP error | "
    if "lcp" in df.columns:
        df.loc[df["lcp"] > 4000, "errors"] += "Poor LCP | "
    if "inp" in df.columns:
        df.loc[df["inp"] > 500, "errors"] += "Poor INP | "
    if "cls" in df.columns:
        df.loc[df["cls"] > 0.25, "errors"] += "High CLS | "
    if "impressions" in df.columns and "clicks" in df.columns:
        df.loc[(df["impressions"] > 1000) & (df["clicks"] == 0), "errors"] += "High impressions but no clicks | "
    df["errors"] = df["errors"].str.strip(" |")
    return df

# -------------------------
# Sort top problem pages first
# -------------------------
def sort_seo_priority(df: pd.DataFrame) -> pd.DataFrame:
    if "errors" in df.columns:
        df["has_error"] = df["errors"].notna() & (df["errors"] != "")
        df_sorted = df.sort_values(
            by=["has_error", "impressions", "clicks"],
            ascending=[False, False, False]
        )
        df_sorted.drop(columns=["has_error"], inplace=True)
        return df_sorted
    return df

# -------------------------
# Generate Gemini summary
# -------------------------
def generate_top_pages_summary(df: pd.DataFrame, limit: int = 10) -> list:
    summaries = []
    if df.empty or "page" not in df.columns:
        return summaries
    top_df = df.sort_values("clicks", ascending=False).head(limit)
    for _, row in top_df.iterrows():
        summaries.append(
            f"Page {row.get('page','N/A')} has {int(row.get('clicks',0))} clicks, "
            f"{int(row.get('impressions',0))} impressions, "
            f"CTR {row.get('ctr',0):.2f}%, "
            f"average position {row.get('position',0):.1f}. "
            f"Errors: {row.get('errors','None')}"
        )
    return summaries

# -------------------------
# Aggregate helpers
# -------------------------
def aggregate_page_metrics(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["page", "clicks", "impressions", "ctr", "position"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0
    return df.groupby("page", dropna=True).agg(
        total_clicks=("clicks", "sum"),
        total_impressions=("impressions", "sum"),
        avg_ctr=("ctr", "mean"),
        avg_position=("position", "mean")
    ).reset_index()

def aggregate_cwv(df: pd.DataFrame) -> pd.DataFrame:
    cwv_cols = [c for c in ["lcp", "inp", "cls"] if c in df.columns]
    if not cwv_cols or "page" not in df.columns:
        return pd.DataFrame()
    return df.groupby("page")[cwv_cols].mean().reset_index()

def aggregate_errors(df: pd.DataFrame) -> pd.DataFrame:
    if "errors" not in df.columns or "page" not in df.columns:
        return pd.DataFrame()
    df["errors"] = df["errors"].fillna("").astype(str)
    df_valid = df[df["errors"].str.strip() != ""]
    if df_valid.empty:
        return pd.DataFrame()
    exploded = df_valid.assign(errors=df_valid["errors"].str.split(" | ")).explode("errors")
    return exploded.groupby(["page", "errors"]).size().reset_index(name="count").sort_values("count", ascending=False)

def build_gemini_summary(page_df, error_df, limit=20) -> str:
    lines = []
    top_pages = page_df.sort_values(
        ["total_impressions", "total_clicks"], ascending=[False, False]
    ).head(limit)
    for _, row in top_pages.iterrows():
        page = row["page"]
        clicks = int(row["total_clicks"])
        imps = int(row["total_impressions"])
        ctr = row["avg_ctr"]
        pos = row["avg_position"]
        errors = error_df[error_df["page"]==page]["errors"].tolist() if not error_df.empty else []
        error_text = ", ".join(errors) if errors else "No critical errors"
        lines.append(f"Page {page} → {imps} impressions, {clicks} clicks, CTR {ctr:.2f}%, Avg position {pos:.1f}. Issues: {error_text}")
    return "\n".join(lines)

# -------------------------
# Process single file (preprocess + aggregate if applicable)
# -------------------------
def process_file(input_path: str, output_base: str, aggregate_files: list):
    try:
        df = pd.read_csv(input_path, engine="python", on_bad_lines="skip")
        if df.empty:
            print(f"⚠️ Skipped empty file: {input_path}")
            return

        df = normalize_columns(df)
        df = detect_seo_errors(df)
        df = sort_seo_priority(df)

        # Save preprocessed
        rel_path = os.path.relpath(input_path, start=r"D:\Final\output")
        output_path = os.path.join(output_base, rel_path)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"✅ Preprocessed: {output_path}")

        # Aggregate only if in the special 3 files
        if os.path.basename(input_path) in aggregate_files and "page" in df.columns:
            page_agg = aggregate_page_metrics(df)
            cwv_agg = aggregate_cwv(df)
            error_agg = aggregate_errors(df)
            if not cwv_agg.empty:
                page_agg = page_agg.merge(cwv_agg, on="page", how="left")

            # Overwrite in same folder
            page_agg.to_csv(output_path, index=False)
            if not error_agg.empty:
                error_agg.to_csv(os.path.join(os.path.dirname(output_path), "error_aggregation.csv"), index=False)

            summary_file = os.path.join(os.path.dirname(output_path), "gemini_aggregation_summary.txt")
            with open(summary_file, "w", encoding="utf-8") as f:
                f.write(build_gemini_summary(page_agg, error_agg))

            print(f"✅ Aggregated: {output_path}")

    except Exception as e:
        print(f"⚠️ Failed: {input_path} → {e}")

# -------------------------
# Main run
# -------------------------
def main():
    output_base = r"D:\Final\preprocessed_outputs"
    input_dirs = [
        r"D:\Final\output\Acquisition Reports",
        r"D:\Final\output\Drive Sales Reports",
        r"D:\Final\output\Engagement Reports",
        r"D:\Final\output\Generate Leads Reports",
        r"D:\Final\output\Monetization Reports",
        r"D:\Final\output\Retention Reports",
        r"D:\Final\output\User Attributes Reports",
        r"D:\Final\output\View User Engagements Reports",
        r"D:\Final\output\GSC Reports\Performance Reports"
    ]
    single_files = [
        r"D:\Final\output\final_pages_indexing_performance_cwv.csv"
    ]
    # Files to aggregate
    aggregate_files = ["Landing page.csv", "pages.csv", "final_pages_indexing_performance_cwv.csv"]

    # Process folders
    for folder in input_dirs:
        csv_files = glob.glob(os.path.join(folder, "**", "*.csv"), recursive=True)
        for f in csv_files:
            process_file(f, output_base, aggregate_files)

    # Process single files
    for f in single_files:
        process_file(f, output_base, aggregate_files)

if __name__ == "__main__":
    main()
