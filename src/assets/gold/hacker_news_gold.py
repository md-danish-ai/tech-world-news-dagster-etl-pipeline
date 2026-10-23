import json
import os
import sqlite3
from datetime import datetime
from typing import Optional
import duckdb
import pandas as pd
from dagster import asset, AssetExecutionContext, MetadataValue

from src.utils.helpers import parse_tokens, IST
from src.utils.models import HNBuzzwordRanked


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

    # ---------------------------------------------------------
    # 6. Observability Metadata for Dagster UI
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
