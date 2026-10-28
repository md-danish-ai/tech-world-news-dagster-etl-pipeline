import json
import os
import sqlite3
from datetime import datetime, timedelta
import altair as alt
import duckdb
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


# -----------------------------------------------------------------------------
# 1. Page Configuration & Custom CSS
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Tech World News • Analytics Dashboard",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded",
)
# Automatically refreshes the browser page every 10 minutes (600,000 ms)
components.html(
    """
    <script>
        setTimeout(function(){
            window.parent.location.reload();
        }, 300000);
    </script>
    """,
    height=0,
)


st.markdown(
    """
    <style>
    .metric-card {
        background: rgba(128, 128, 128, 0.08);
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .badge-rank-1 { background-color: #FEF08A; color: #854D0E; padding: 3px 8px; border-radius: 6px; font-weight: bold; }
    .badge-rank-2 { background-color: #E2E8F0; color: #334155; padding: 3px 8px; border-radius: 6px; font-weight: bold; }
    .badge-rank-3 { background-color: #FED7AA; color: #9A3412; padding: 3px 8px; border-radius: 6px; font-weight: bold; }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# 2. Database Connection & Data Loaders
# -----------------------------------------------------------------------------
DUCKDB_PATH = os.path.join("data", "gold", "gold_analytics.duckdb")
SIGNAL_PATH = os.path.join("data", ".refresh_signal.json")
SILVER_SQLITE_PATH = os.path.join("data", "silver", "hackernews_silver.sqlite")
BRONZE_SQLITE_PATH = os.path.join("data", "bronze", "hackernews_bronze.sqlite")


def get_refresh_signal():
    if os.path.exists(SIGNAL_PATH):
        try:
            with open(SIGNAL_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


@st.cache_data(ttl=300)
def load_duckdb_data():
    """Loads tables from DuckDB with error resilience."""
    if not os.path.exists(DUCKDB_PATH):
        return pd.DataFrame(), pd.DataFrame(), f"DuckDB file not found at {DUCKDB_PATH}"

    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]

        buzzwords_df = (
            conn.execute("SELECT * FROM top_trending_topics ORDER BY rank ASC").df()
            if "top_trending_topics" in tables
            else pd.DataFrame()
        )

        stories_df = (
            conn.execute("SELECT * FROM trending_stories_by_score ORDER BY score DESC").df()
            if "trending_stories_by_score" in tables
            else pd.DataFrame()
        )

        conn.close()
        return buzzwords_df, stories_df, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), str(e)


def get_layer_counts():
    """Returns row counts from Bronze, Silver, and Gold for pipeline observability."""
    counts = {"bronze": 0, "silver": 0, "gold_buzzwords": 0, "gold_stories": 0}
    try:
        if os.path.exists(BRONZE_SQLITE_PATH):
            with sqlite3.connect(BRONZE_SQLITE_PATH) as conn:
                counts["bronze"] = conn.execute("SELECT COUNT(*) FROM bronze_raw_stories").fetchone()[0]
    except Exception:
        pass

    try:
        if os.path.exists(SILVER_SQLITE_PATH):
            with sqlite3.connect(SILVER_SQLITE_PATH) as conn:
                counts["silver"] = conn.execute("SELECT COUNT(*) FROM silver_common_table").fetchone()[0]
    except Exception:
        pass

    try:
        if os.path.exists(DUCKDB_PATH):
            with duckdb.connect(DUCKDB_PATH, read_only=True) as conn:
                tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
                if "top_trending_topics" in tables:
                    counts["gold_buzzwords"] = conn.execute("SELECT COUNT(*) FROM top_trending_topics").fetchone()[0]
                if "trending_stories_by_score" in tables:
                    counts["gold_stories"] = conn.execute("SELECT COUNT(*) FROM trending_stories_by_score").fetchone()[0]
    except Exception:
        pass

    return counts


# -----------------------------------------------------------------------------
# 3. Sidebar: Observability & Global Filters
# -----------------------------------------------------------------------------
buzzwords_df, stories_df, error_msg = load_duckdb_data()
signal_data = get_refresh_signal()

with st.sidebar:
    logo_path = "dagster-logo.png"
    if os.path.exists(logo_path):
        st.image(logo_path, width=120)
    else:
        st.image("https://cdn.jsdelivr.net/gh/homarr-labs/dashboard-icons/png/dagster.png", width=120)
    st.markdown("### ⚙️ Pipeline Control")

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.markdown("#### 🕒 Last ETL Run")
    if signal_data:
        st.success(f"**Refreshed:** {signal_data.get('last_refreshed_at', 'N/A')}")
        st.caption(f"**Latest Asset:** `{signal_data.get('latest_asset', 'N/A')}`")
        st.caption(f"**Run ID:** `{signal_data.get('dagster_run_id', 'N/A')[:8]}...`")
    else:
        st.info("No refresh signal file detected yet. Run the Dagster Gold assets to generate.")

    st.markdown("---")
    st.markdown("#### 🔍 Filter Stories")

    # Time Horizon Filter
    time_filter = st.selectbox(
        "📅 Time Horizon",
        options=["All Time", "Past 24 Hours (Today)", "Past 7 Days (Week)", "Past 30 Days (Month)"],
        index=0,
        help="Filter stories based on when they were published on Hacker News.",
    )

    # Keyword / Author search
    search_query = st.text_input("🔎 Search Title or Author", placeholder="e.g. AI, Python, whoishiring")

    # Score Filter
    max_score = int(stories_df["score"].max()) if not stories_df.empty else 100
    min_score = st.slider("⭐ Minimum Upvote Score", min_value=0, max_value=max(10, max_score), value=0, step=10)

    # Limit Filter
    limit_count = st.slider("📊 Top Stories to Display", min_value=5, max_value=100, value=25, step=5)

    st.markdown("---")
    st.markdown("🔗 **Quick Links:**")
    st.markdown("- [Dagster UI (localhost:6500)](http://localhost:6500)")
    st.markdown("- [SQLite Web UI (localhost:8085)](http://localhost:8085)")


# -----------------------------------------------------------------------------
# 4. Main Page Header
# -----------------------------------------------------------------------------
st.title("📰 Tech World News Analytics")
st.markdown(
    "##### *Real-time Hacker News intelligence curated by Dagster ETL and queried with DuckDB*"
)
st.write("")

if error_msg:
    st.warning(f"⚠️ {error_msg}. Please materialize your assets in Dagster UI first.")

# -----------------------------------------------------------------------------
# 5. Tabbed Interface
# -----------------------------------------------------------------------------
tab_stories, tab_buzzwords, tab_arch = st.tabs([
    "🏆 Trending Stories by Score",
    "🔥 Top Tech Buzzwords",
    "🏗️ Architecture & Health",
])

# -----------------------------------------------------------------------------
# TAB 1: Trending Stories by Score
# -----------------------------------------------------------------------------
with tab_stories:
    if stories_df.empty:
        st.info("No stories found in DuckDB. Run the `trending_stories_by_score` asset in Dagster UI.")
    else:
        filtered_df = stories_df.copy()

        # Parse published_at_dt for temporal filtering
        if "published_at_dt" in filtered_df.columns:
            filtered_df["dt_parsed"] = pd.to_datetime(filtered_df["published_at_dt"], errors="coerce")
        else:
            filtered_df["dt_parsed"] = pd.to_datetime(
                filtered_df["published_at"].str.replace(r"\s+IST$", "", regex=True), errors="coerce"
            )

        # Apply Time Horizon Filter
        if not filtered_df["dt_parsed"].dropna().empty:
            max_dt = filtered_df["dt_parsed"].max()
            if time_filter == "Past 24 Hours (Today)":
                filtered_df = filtered_df[filtered_df["dt_parsed"] >= (max_dt - timedelta(days=1))]
            elif time_filter == "Past 7 Days (Week)":
                filtered_df = filtered_df[filtered_df["dt_parsed"] >= (max_dt - timedelta(days=7))]
            elif time_filter == "Past 30 Days (Month)":
                filtered_df = filtered_df[filtered_df["dt_parsed"] >= (max_dt - timedelta(days=30))]

        # Apply Score & Search Filters
        filtered_df = filtered_df[filtered_df["score"] >= min_score]

        if search_query.strip():
            sq = search_query.strip().lower()
            filtered_df = filtered_df[
                filtered_df["title"].str.lower().str.contains(sq, na=False)
                | filtered_df["author"].str.lower().str.contains(sq, na=False)
            ]

        # Re-sort and slice Top N
        filtered_df = filtered_df.sort_values(by="score", ascending=False).reset_index(drop=True)
        filtered_df["current_rank"] = range(1, len(filtered_df) + 1)
        display_df = filtered_df.head(limit_count)

        # Metrics Row
        col1, col2, col3, col4 = st.columns(4)
        top_story = display_df.iloc[0] if not display_df.empty else None

        col1.metric("📰 Stories Matching Filter", len(filtered_df))
        col2.metric("⭐ Highest Score", f"{int(top_story['score'])} pts" if top_story is not None else "0 pts")
        col3.metric(
            "📊 Average Score",
            f"{filtered_df['score'].mean():.1f} pts" if not filtered_df.empty else "0 pts",
        )
        col4.metric(
            "🥇 Top Story",
            f"{top_story['title'][:28]}..." if top_story is not None else "N/A",
            help=top_story["title"] if top_story is not None else "",
        )

        st.markdown(f"#### 📋 Leaderboard: Top {len(display_df)} Stories ({time_filter})")

        # Interactive Chart of Top Stories
        if not display_df.empty:
            chart = (
                alt.Chart(display_df.head(15))
                .mark_bar(cornerRadiusTopRight=5, cornerRadiusBottomRight=5)
                .encode(
                    x=alt.X("score:Q", title="Upvote Score"),
                    y=alt.Y("title:N", sort="-x", title="", axis=alt.Axis(labelLimit=350)),
                    color=alt.Color("score:Q", scale=alt.Scale(scheme="blues"), legend=None),
                    tooltip=["current_rank", "title", "score", "author", "published_at"],
                )
                .properties(height=min(450, 30 * len(display_df.head(15)) + 40))
            )
            st.altair_chart(chart, use_container_width=True)

        # Formatted Display Table
        table_rows = []
        for _, row in display_df.iterrows():
            r = row["current_rank"]
            rank_str = f"🥇 {r}" if r == 1 else (f"🥈 {r}" if r == 2 else (f"🥉 {r}" if r == 3 else f"#{r}"))
            url = row.get("url") or f"https://news.ycombinator.com/item?id={row.get('story_id')}"
            link_title = f"[{row['title']}]({url})"

            table_rows.append({
                "Rank": rank_str,
                "Title": link_title,
                "Score (Upvotes)": f"⭐ {row['score']}",
                "Author": f"👤 {row['author']}",
                "Published (IST)": row.get("published_at", "N/A"),
            })

        table_df = pd.DataFrame(table_rows)
        st.markdown(table_df.to_markdown(index=False), unsafe_allow_html=True)

        # CSV Export
        st.markdown("---")
        csv_data = filtered_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download Filtered Stories as CSV",
            data=csv_data,
            file_name=f"hackernews_trending_stories_{time_filter.lower().replace(' ', '_')}.csv",
            mime="text/csv",
        )


# -----------------------------------------------------------------------------
# TAB 2: Top Tech Buzzwords
# -----------------------------------------------------------------------------
with tab_buzzwords:
    if buzzwords_df.empty:
        st.info("No buzzwords found in DuckDB. Run the `top_trending_topics` asset in Dagster UI.")
    else:
        # Metrics Row
        col1, col2, col3 = st.columns(3)
        top_bw = buzzwords_df.iloc[0] if not buzzwords_df.empty else None

        col1.metric("🔥 #1 Trending Keyword", f"#{top_bw['keyword'].upper()}" if top_bw is not None else "N/A")
        col2.metric("💬 Mentions of #1 Word", f"{top_bw['mention_count']} times" if top_bw is not None else "0")
        col3.metric("📊 Total Ranked Keywords", len(buzzwords_df))

        st.markdown("#### 📈 Keyword Frequency Leaderboard (Top 20)")

        # Altair Horizontal Bar Chart
        bar_chart = (
            alt.Chart(buzzwords_df)
            .mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
            .encode(
                x=alt.X("mention_count:Q", title="Mention Count"),
                y=alt.Y("keyword:N", sort="-x", title=""),
                color=alt.Color("mention_count:Q", scale=alt.Scale(scheme="oranges"), legend=None),
                tooltip=["rank", "keyword", "mention_count", "computed_at"],
            )
            .properties(height=500)
        )
        st.altair_chart(bar_chart, use_container_width=True)

        # Buzzword Table
        st.dataframe(
            buzzwords_df[["rank", "keyword", "mention_count", "computed_at"]],
            column_config={
                "rank": "Rank",
                "keyword": "Tech Keyword / Buzzword",
                "mention_count": st.column_config.ProgressColumn(
                    "Story Mentions",
                    format="%d",
                    min_value=0,
                    max_value=int(buzzwords_df["mention_count"].max()),
                ),
                "computed_at": "Analyzed At",
            },
            hide_index=True,
            use_container_width=True,
        )


# -----------------------------------------------------------------------------
# TAB 3: Architecture & Pipeline Health
# -----------------------------------------------------------------------------
with tab_arch:
    st.markdown("### 🏗️ Medallion Architecture (Bronze ➔ Silver ➔ Gold)")
    counts = get_layer_counts()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🥉 Bronze Stories (SQLite)", counts["bronze"])
    c2.metric("🥈 Silver Stories (SQLite)", counts["silver"])
    c3.metric("🥇 Gold Buzzwords (DuckDB)", counts["gold_buzzwords"])
    c4.metric("🥇 Gold Ranked Stories (DuckDB)", counts["gold_stories"])

    st.markdown(
        """
        - **Bronze Layer (`data/bronze/hackernews_bronze.sqlite`)**: Ingests raw JSON payloads from Hacker News REST API.
        - **Silver Layer (`data/silver/hackernews_silver.sqlite`)**: Cleans, deduplicates with SHA-256 hashes, normalizes schema, and tokenizes titles.
        - **Gold Layer (`data/gold/gold_analytics.duckdb`)**: DuckDB analytical store for high-performance dashboard querying:
          - `top_trending_topics`: Frequency count of cleaned tokens.
          - `trending_stories_by_score`: Stories ordered by upvotes, standardized timestamps for day/week/month querying.
        """
    )
