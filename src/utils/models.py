from typing import Optional
from pydantic import BaseModel


class HNStoryCleaned(BaseModel):
    """Pydantic schema model enforcing strict types for Silver layer data."""
    story_id: int
    title: str
    title_hash: str
    title_cleaned: str
    cleaned_tokens: str
    author: str = "unknown"
    score: int = 0
    url: Optional[str] = ""
    published_at: Optional[str] = None
    source: str = "hn_api"
    transfer_timestamp: str
    dagster_run_id: str

class HNBuzzwordRanked(BaseModel):
    """Pydantic model enforcing schema for Gold Top 20 Buzzwords."""
    rank: int
    keyword: str
    mention_count: int
    computed_at: str
    dagster_run_id: str
