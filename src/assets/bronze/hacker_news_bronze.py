import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta
import pandas as pd
from dagster import asset, AssetExecutionContext, MetadataValue, RetryPolicy
from src.resources.hn_api import HackerNewsResource


@asset(
    group_name="bronze",
    description="Ingests raw Hacker News stories directly into SQLite ",
    retry_policy=RetryPolicy(max_retries=3, delay=60)
)

def raw_top_stories(context: AssetExecutionContext, hn_api: HackerNewsResource) -> pd.DataFrame:
    # 1. Fetch raw payloads from the API
    limit = 50
    context.log.info(f"Extracting top {limit} stories from Hacker News API...")
    raw_stories = hn_api.get_top_stories(limit=limit)

    # 2. Guard: Ensure we didn't receive an empty batch
    if not raw_stories:
        raise ValueError("Fatal: 0 stories were extracted from the Hacker News API!")

    context.log.info(f"Successfully downloaded {len(raw_stories)} raw stories.")

    # 3. Put straight into a Pandas DataFrame (Zero transformation)
    df = pd.DataFrame(raw_stories)

    # 4. Append Audit Columns in IST (Indian Standard Time: UTC + 05:30)
    IST = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    df["extracted_at"] = now_ist

    df["dagster_run_id"] = context.run_id

    # 5. Serialize nested array 'kids' to JSON string for SQLite storage
    if "kids" in df.columns:
        df["kids"] = df["kids"].apply(lambda k: json.dumps(k) if isinstance(k, list) else None)

    # 6. Save to local SQLite storage in bronze layer folder
    db_path = os.path.join("data", "bronze", "hackernews_bronze.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        df.to_sql("bronze_raw_stories", con=conn, if_exists="append", index=False)

    context.log.info(f"Saved {len(df)} records into SQLite table 'bronze_raw_stories'.")

    # 7. Attach Observability Metadata for the Dagster UI
    preview_cols = [col for col in ["id", "title", "by", "score", "extracted_at"] if col in df.columns]
    context.add_output_metadata({
        "records_ingested": MetadataValue.int(len(df)),
        "database_path": MetadataValue.path(db_path),
        "database_table": MetadataValue.text("bronze_raw_stories"),
        "extracted_at": MetadataValue.text(now_ist),
        "columns_captured": MetadataValue.text(", ".join(df.columns.tolist())),
        # "preview": MetadataValue.md(df[preview_cols].to_markdown(index=False)),
        "all_50_stories_table": MetadataValue.md(df.to_markdown(index=False)),
    })

    # 8. Return DataFrame for downstream assets
    return df
