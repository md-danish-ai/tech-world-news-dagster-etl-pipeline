import os
import json
import time
import sqlite3
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo
import duckdb
import pandas as pd
from dagster import asset, AssetExecutionContext, MetadataValue  
from src.assets.silver.hacker_news_silver import silver_common_table
from src.utils.helpers import IST, parse_tokens
from src.utils.models import HNBuzzwordRanked, HNTrendingStoryRanked

def _connect_duckdb_with_retry(db_path: str, context: AssetExecutionContext, max_attempts: int = 5) -> duckdb.DuckDBPyConnection:
    """Safely connects to DuckDB with retry handling in case of lingering file locks."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    for attempt in range(max_attempts):
        try:
            return duckdb.connect(db_path)
        except Exception as e:
            if "Could not set lock" in str(e) and attempt < max_attempts - 1:
                context.log.warning(
                    f"DuckDB database locked by another process. Retrying in 2 seconds... (Attempt {attempt + 1}/{max_attempts})"
                )
                time.sleep(2)
            else:
                raise e

def write_refresh_signal(run_id: str, asset_name: str) -> None:
    """Writes a lightweight refresh signal file indicating Gold layer materialization for dashboard auto-refresh."""
    signal_path = os.path.join("data", ".refresh_signal.json")
    os.makedirs(os.path.dirname(signal_path), exist_ok=True)
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    payload = {
        "last_refreshed_at": now_ist,
        "latest_asset": asset_name,
        "dagster_run_id": run_id,
        "timestamp_epoch": int(datetime.now().timestamp()),
    }
    with open(signal_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


@asset(
    group_name="gold",
    description="Ranks the Top 20 trending tech buzzwords and stores in DuckDB analytical database.",
)
def top_trending_topics(context: AssetExecutionContext, silver_common_table: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    silver_db_path = os.path.join("data", "silver", "hackernews_silver.sqlite")
    gold_db_path = os.path.join("data", "gold", "gold_analytics.duckdb")

    # 1. Fallback: Load from Silver database if DataFrame is not in memory
    if silver_common_table is None or silver_common_table.empty:
        if os.path.exists(silver_db_path):
            context.log.info(f"Loading cleaned stories from Silver database: {silver_db_path}")
            with sqlite3.connect(silver_db_path) as conn:
                silver_common_table = pd.read_sql("SELECT * FROM silver_common_table", conn)
        else:
            raise ValueError(f"Silver database not found at {silver_db_path}!")

    total_stories = len(silver_common_table)
    context.log.info(f"Analyzing keywords across {total_stories} stories from Silver layer...")

    df = silver_common_table.copy()

    # 2. Unpack tokens using helper
    words_series = df["cleaned_tokens"].apply(parse_tokens).explode()
    words_clean = words_series.dropna()
    words_clean = words_clean[words_clean.astype(str).str.strip().str.len() > 1]

    if words_clean.empty:
        context.log.warning("No keywords found to analyze!")
        return pd.DataFrame()

    # 3. Pure Frequency Count (+1 per occurrence)
    freq_df = words_clean.value_counts().reset_index()
    freq_df.columns = ["keyword", "mention_count"]

    # Select Top 20 Buzzwords and assign rank
    top_20 = freq_df.head(20).copy()
    top_20["rank"] = range(1, len(top_20) + 1)

    # Add Audit Metadata in IST
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    top_20["computed_at"] = now_ist
    top_20["dagster_run_id"] = context.run_id

    # 4. Pydantic Schema Validation
    validated_records = []
    for row in top_20.to_dict(orient="records"):
        try:
            validated = HNBuzzwordRanked.model_validate(row)
            validated_records.append(validated.model_dump())
        except Exception as e:
            context.log.warning(f"Validation error for buzzword row: {e}")

    gold_df = pd.DataFrame(validated_records)

    # ---------------------------------------------------------
    # 5. Save to DuckDB: data/gold/gold_analytics.duckdb
    # ---------------------------------------------------------
    os.makedirs(os.path.dirname(gold_db_path), exist_ok=True)
    with duckdb.connect(gold_db_path) as conn:
        conn.execute("CREATE OR REPLACE TABLE top_trending_topics AS SELECT * FROM gold_df")

    context.log.info(f"Saved Top {len(gold_df)} buzzwords into DuckDB: {gold_db_path} ('top_trending_topics').")

    # 6. Write Dashboard Refresh Signal
    write_refresh_signal(context.run_id, "top_trending_topics")

    # ---------------------------------------------------------
    # 7. Observability Metadata for Dagster UI
    # ---------------------------------------------------------
    preview_cols = ["rank", "keyword", "mention_count"]
    top_word = gold_df.iloc[0]["keyword"] if not gold_df.empty else "N/A"

    context.add_output_metadata({
        "number_one_trending_buzzword": MetadataValue.text(f"#{top_word.upper()}"),
        "total_unique_keywords_analyzed": MetadataValue.int(len(freq_df)),
        "database_type": MetadataValue.text("DuckDB (OLAP Analytics)"),
        "database_file": MetadataValue.path(gold_db_path),
        "database_table": MetadataValue.text("top_trending_topics"),
        "computed_at": MetadataValue.text(now_ist),
        "top_20_buzzwords_preview": MetadataValue.md(gold_df[preview_cols].to_markdown(index=False)),
    })

    return gold_df


@asset(
    group_name="gold",
    # This guarantees Dagster runs them one-by-one so DuckDB never locks!
    deps=[silver_common_table, top_trending_topics],
    description="Ranks Hacker News stories by upvote score with standardized timestamps for day/week/month analytical filtering in DuckDB.",
)
def trending_stories_by_score(context: AssetExecutionContext, silver_common_table: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    silver_db_path = os.path.join("data", "silver", "hackernews_silver.sqlite")
    gold_db_path = os.path.join("data", "gold", "gold_analytics.duckdb")

    # 1. Fallback: Load from Silver database if DataFrame is not in memory
    if silver_common_table is None or silver_common_table.empty:
        if os.path.exists(silver_db_path):
            context.log.info(f"Loading cleaned stories from Silver database: {silver_db_path}")
            with sqlite3.connect(silver_db_path) as conn:
                silver_common_table = pd.read_sql("SELECT * FROM silver_common_table", conn)
        else:
            raise ValueError(f"Silver database not found at {silver_db_path}!")

    total_stories = len(silver_common_table)
    context.log.info(f"Processing and ranking {total_stories} stories by score...")

    df = silver_common_table.copy()
    if df.empty:
        context.log.warning("No stories available from Silver layer to rank!")
        return pd.DataFrame()

    # 2. Score cleansing and numeric sorting
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)

    # 3. Standardize published_at_dt (ISO timestamp YYYY-MM-DD HH:MM:SS) for seamless DuckDB temporal queries
    if "published_at" in df.columns:
        # Strip trailing ' IST' if present to produce standard ISO format
        clean_ts = df["published_at"].astype(str).str.replace(r"\s+IST$", "", regex=True)
        # Convert to datetime and format as standard string
        df["published_at_dt"] = pd.to_datetime(clean_ts, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        df["published_at"] = None
        df["published_at_dt"] = None

    # Sort stories by score DESC, then published_at DESC
    df = df.sort_values(by=["score", "published_at"], ascending=[False, False]).reset_index(drop=True)

    # Assign analytical score rank (1 to N)
    df["rank"] = range(1, len(df) + 1)

    # Add audit metadata in IST
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    df["computed_at"] = now_ist
    df["dagster_run_id"] = context.run_id

    # 4. Select and Validate schema using Pydantic HNTrendingStoryRanked
    target_cols = [
        "rank", "story_id", "title", "author", "score",
        "url", "published_at", "published_at_dt", "computed_at", "dagster_run_id"
    ]
    # Ensure any missing optional columns are initialized
    for col in target_cols:
        if col not in df.columns:
            df[col] = "" if col == "url" else None

    validated_records = []
    for row in df[target_cols].to_dict(orient="records"):
        try:
            validated = HNTrendingStoryRanked.model_validate(row)
            validated_records.append(validated.model_dump())
        except Exception as e:
            context.log.warning(f"Validation error for story row {row.get('story_id')}: {e}")

    gold_df = pd.DataFrame(validated_records)

    # ---------------------------------------------------------
    # 5. Save to DuckDB: data/gold/gold_analytics.duckdb
    # ---------------------------------------------------------
    os.makedirs(os.path.dirname(gold_db_path), exist_ok=True)
    with _connect_duckdb_with_retry(gold_db_path, context) as conn:
        conn.execute("CREATE OR REPLACE TABLE trending_stories_by_score AS SELECT * FROM gold_df")

    context.log.info(f"Saved {len(gold_df)} trending stories into DuckDB table 'trending_stories_by_score'.")

    # 6. Write Dashboard Refresh Signal
    write_refresh_signal(context.run_id, "trending_stories_by_score")

    # ---------------------------------------------------------
    # 7. Observability Metadata for Dagster UI
    # ---------------------------------------------------------
    top_story = gold_df.iloc[0] if not gold_df.empty else None
    top_story_title = top_story["title"] if top_story is not None else "N/A"
    top_story_score = int(top_story["score"]) if top_story is not None else 0
    avg_score = round(float(gold_df["score"].mean()), 1) if not gold_df.empty else 0.0

    preview_cols = ["rank", "story_id", "title", "score", "author", "published_at"]
    display_cols = [c for c in preview_cols if c in gold_df.columns]

    context.add_output_metadata({
        "number_one_ranked_story": MetadataValue.text(f"'{top_story_title}' ({top_story_score} pts)"),
        "total_stories_ranked": MetadataValue.int(len(gold_df)),
        "average_score": MetadataValue.float(avg_score),
        "database_type": MetadataValue.text("DuckDB (OLAP Analytics)"),
        "database_file": MetadataValue.path(gold_db_path),
        "database_table": MetadataValue.text("trending_stories_by_score"),
        "computed_at": MetadataValue.text(now_ist),
        "top_10_stories_preview": MetadataValue.md(
            gold_df[display_cols].head(10).to_markdown(index=False) if not gold_df.empty else "No stories"
        ),
    })

    return gold_df
