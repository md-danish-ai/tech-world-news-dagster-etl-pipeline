import os
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional
import pandas as pd
from dagster import asset, AssetExecutionContext, MetadataValue, RetryPolicy

# Import reusable helpers and Pydantic validation model
from src.utils.helpers import (
    clean_title_text,
    tokenize_and_filter_stopwords,
    compute_sha256,
    format_epoch_to_ist,
)
from src.utils.models import HNStoryCleaned


@asset(
    group_name="silver",
    description="Standardizes, hashes (SHA-256), deduplicates, cleans titles, validates schemas, and saves to data/silver/hackernews_silver.sqlite",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
)
def silver_common_table(context: AssetExecutionContext, raw_top_stories: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    # ---------------------------------------------------------
    # STEP 1: Ingest & Guard Check
    # ---------------------------------------------------------
    bronze_db_path = os.path.join("data", "bronze", "hackernews_bronze.sqlite")
    silver_db_path = os.path.join("data", "silver", "hackernews_silver.sqlite")

    # Resilient fallback: If raw_top_stories is not provided via I/O manager,
    # load directly from the bronze table in the SQLite database.
    if raw_top_stories is None or raw_top_stories.empty:
        if os.path.exists(bronze_db_path):
            context.log.info(f"Loading raw stories from bronze table in: {bronze_db_path}")
            with sqlite3.connect(bronze_db_path) as conn:
                raw_top_stories = pd.read_sql("SELECT * FROM bronze_raw_stories", conn)
        else:
            raise ValueError(f"Bronze SQLite database not found at {bronze_db_path}!")

    total_incoming = len(raw_top_stories)
    context.log.info(f"Received {total_incoming} raw stories from Bronze layer.")

    df = raw_top_stories.copy()

    # Drop records with missing or empty titles
    df = df.dropna(subset=["title"])
    df = df[df["title"].astype(str).str.strip() != ""]

    # ---------------------------------------------------------
    # STEP 2: Schema Standardization & Normalization
    # ---------------------------------------------------------
    # 2.1 Standardize column names (id -> story_id, by -> author)
    rename_mapping = {
        "id": "story_id",
        "by": "author",
    }
    df = df.rename(columns=rename_mapping)

    # 2.2 Convert UNIX epoch 'time' to human-readable IST timestamp
    if "time" in df.columns:
        df["published_at"] = df["time"].apply(format_epoch_to_ist)
    else:
        df["published_at"] = None

    # 2.3 Graceful null handling & defaults
    if "url" not in df.columns:
        df["url"] = ""
    else:
        df["url"] = df["url"].fillna("")

    if "score" not in df.columns:
        df["score"] = 0
    else:
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)

    if "author" not in df.columns:
        df["author"] = "unknown"
    else:
        df["author"] = df["author"].fillna("unknown")

    # 2.4 Add Audit & Lineage Metadata
    IST = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    df["transfer_timestamp"] = now_ist
    df["dagster_run_id"] = context.run_id
    df["source"] = "hn_api"

    # ---------------------------------------------------------
    # STEP 3: SHA-256 Title Hashing & Deduplication
    # ---------------------------------------------------------
    df["title_hash"] = df["title"].apply(compute_sha256)

    # In-batch deduplication (keep first occurrence in incoming batch)
    records_before_batch_dedup = len(df)
    df = df.drop_duplicates(subset=["title_hash"], keep="first")
    batch_duplicates_dropped = records_before_batch_dedup - len(df)
    if batch_duplicates_dropped > 0:
        context.log.info(f"Dropped {batch_duplicates_dropped} duplicate stories within the incoming batch.")

    # ---------------------------------------------------------
    # STEP 4: Title Text Cleaning & Tokenization (NLP)
    # ---------------------------------------------------------
    df["title_cleaned"] = df["title"].apply(clean_title_text)
    df["cleaned_tokens"] = df["title_cleaned"].apply(tokenize_and_filter_stopwords)

    # ---------------------------------------------------------
    # STEP 5: Pydantic Schema Validation & Save to SQLite
    # ---------------------------------------------------------
    os.makedirs(os.path.dirname(silver_db_path), exist_ok=True)

    with sqlite3.connect(silver_db_path) as conn:
        cursor = conn.cursor()

        # 5.1 Ensure silver_common_table exists with Primary Key and Index
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS silver_common_table (
                story_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                title_hash TEXT NOT NULL,
                title_cleaned TEXT,
                cleaned_tokens TEXT,
                author TEXT,
                score INTEGER,
                url TEXT,
                published_at TEXT,
                source TEXT,
                transfer_timestamp TEXT,
                dagster_run_id TEXT
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_title_hash ON silver_common_table(title_hash);")

        # 5.2 Fetch existing title hashes to avoid re-inserting stories with different IDs but identical titles
        existing_hash_rows = cursor.execute("SELECT title_hash, story_id FROM silver_common_table;").fetchall()
        existing_hash_map = {row[0]: row[1] for row in existing_hash_rows}

        valid_records = []
        skipped_duplicate_titles = 0

        for row in df.to_dict(orient="records"):
            t_hash = row["title_hash"]
            s_id = row["story_id"]

            if t_hash in existing_hash_map and existing_hash_map[t_hash] != s_id:
                skipped_duplicate_titles += 1
                continue

            # Validate schema and types using Pydantic model
            try:
                validated_story = HNStoryCleaned.model_validate(row)
                valid_records.append(validated_story.model_dump())
            except Exception as e:
                context.log.warning(f"Skipping malformed row story_id={s_id}: {e}")

        if skipped_duplicate_titles > 0:
            context.log.info(f"Skipped {skipped_duplicate_titles} stories with duplicate titles already in silver table.")

        # 5.3 Upsert into silver_common_table (Insert new, or Update score/timestamps for existing story_id)
        upsert_query = """
            INSERT INTO silver_common_table (
                story_id, title, title_hash, title_cleaned, cleaned_tokens,
                author, score, url, published_at, source, transfer_timestamp, dagster_run_id
            ) VALUES (
                :story_id, :title, :title_hash, :title_cleaned, :cleaned_tokens,
                :author, :score, :url, :published_at, :source, :transfer_timestamp, :dagster_run_id
            )
            ON CONFLICT(story_id) DO UPDATE SET
                title = excluded.title,
                title_hash = excluded.title_hash,
                title_cleaned = excluded.title_cleaned,
                cleaned_tokens = excluded.cleaned_tokens,
                author = excluded.author,
                score = excluded.score,
                url = excluded.url,
                transfer_timestamp = excluded.transfer_timestamp,
                dagster_run_id = excluded.dagster_run_id;
        """

        if valid_records:
            cursor.executemany(upsert_query, valid_records)
            conn.commit()

        total_silver_in_db = cursor.execute("SELECT COUNT(*) FROM silver_common_table;").fetchone()[0]

    context.log.info(
        f"Saved {len(valid_records)} records into 'silver_common_table' in {silver_db_path}. Total in silver table: {total_silver_in_db}."
    )

    # ---------------------------------------------------------
    # STEP 6: Metadata & Observability for Dagster UI
    # ---------------------------------------------------------
    preview_df = pd.DataFrame(valid_records)
    preview_cols = [c for c in ["story_id", "title", "title_cleaned", "cleaned_tokens", "score", "published_at"] if c in preview_df.columns]

    context.add_output_metadata({
        "total_incoming_records": MetadataValue.int(total_incoming),
        "records_upserted": MetadataValue.int(len(valid_records)),
        "skipped_duplicate_titles": MetadataValue.int(batch_duplicates_dropped + skipped_duplicate_titles),
        "total_records_in_silver_db": MetadataValue.int(total_silver_in_db),
        "database_file": MetadataValue.path(silver_db_path),
        "database_table": MetadataValue.text("silver_common_table"),
        "processed_at": MetadataValue.text(now_ist),
        "sample_preview": MetadataValue.md(preview_df[preview_cols].head(8).to_markdown(index=False) if not preview_df.empty else "No records"),
    })

    return preview_df
