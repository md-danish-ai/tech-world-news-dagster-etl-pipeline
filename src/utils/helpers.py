import hashlib
import json
import re
from datetime import datetime, timezone, timedelta
from typing import Optional
import pandas as pd

# IST Timezone (UTC + 05:30)
IST = timezone(timedelta(hours=5, minutes=30))

# Standard Stopwords to filter out non-informative words from technology headlines
STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of",
    "with", "by", "from", "up", "about", "into", "over", "after", "is", "are",
    "was", "were", "be", "been", "being", "have", "has", "had", "do", "does",
    "did", "will", "would", "shall", "should", "may", "might", "must", "can",
    "could", "it", "its", "this", "that", "these", "those", "i", "you", "he",
    "she", "we", "they", "what", "which", "who", "whom", "how", "when", "where",
    "why", "as", "if", "not", "no", "all", "any", "some", "more", "most", "other",
    "so", "than", "too", "very", "s", "t", "just", "don", "now", "hn", "show", "ask"
}


def clean_title_text(title: str) -> str:
    """Converts title to lowercase, removes punctuation/symbols, and collapses whitespace."""
    if not title or pd.isna(title):
        return ""
    text = str(title).lower()
    # Strip URL links if present
    text = re.sub(r"https?://\S+|www\.\S+", "", text)
    # Strip punctuation and special characters (keep only alphanumeric and whitespace)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    # Collapse multiple whitespaces
    return re.sub(r"\s+", " ", text).strip()


def tokenize_and_filter_stopwords(cleaned_text: str) -> str:
    """Extracts meaningful words excluding stopwords and serializes to a JSON string."""
    if not cleaned_text:
        return json.dumps([])
    words = cleaned_text.split()
    meaningful_tokens = [w for w in words if len(w) > 1 and w not in STOPWORDS]
    return json.dumps(meaningful_tokens)


def compute_sha256(text: str) -> str:
    """Computes SHA-256 hash for each story title."""
    if not text or pd.isna(text):
        return ""
    return hashlib.sha256(str(text).strip().encode("utf-8")).hexdigest()


def format_epoch_to_ist(epoch_val) -> Optional[str]:
    """Converts UNIX epoch 'time' to human-readable IST timestamp."""
    if pd.isna(epoch_val) or epoch_val is None:
        return None
    try:
        return datetime.fromtimestamp(int(epoch_val), tz=IST).strftime("%Y-%m-%d %H:%M:%S IST")
    except Exception:
        return None


def parse_tokens(val) -> list:
    """Safely parses JSON string or list of tokens from SQLite."""
    if isinstance(val, list):
        return val
    if isinstance(val, str) and val.strip():
        try:
            return json.loads(val)
        except Exception:
            return []
    return []
