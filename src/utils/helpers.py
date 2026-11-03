import hashlib
import json
import re
from datetime import datetime, timezone, timedelta
from typing import Optional
import pandas as pd
import spacy

# IST Timezone (UTC + 05:30)
IST = timezone(timedelta(hours=5, minutes=30))

# Load spaCy model once at module level (with self-healing fallback)
try:
    _nlp = spacy.load("en_core_web_sm")
except Exception:
    import spacy.cli
    spacy.cli.download("en_core_web_sm")
    _nlp = spacy.load("en_core_web_sm")

# ---------------------------------------------------------------------------
# STEP 4: Generic Stopwords & Non-Technical Term Exclusions
# Configurable list to filter common English words and borderline non-tech terms.
# ---------------------------------------------------------------------------
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

GENERIC_TERMS = {
    "problem", "count", "case", "thing", "things", "first", "new", "world",
    "come", "war", "pico", "hours", "same", "year", "years", "day", "days",
    "time", "times", "life", "water", "way", "ways", "use", "cost", "power",
    "numbers", "stats", "page", "people", "man", "men", "woman", "women",
    "farmer", "attic", "baking", "paper", "video", "history", "book", "books"
}

# ---------------------------------------------------------------------------
# STEP 3: Technical Vocabulary Dictionary
# Curated dictionary of known technologies, frameworks, tools, and concepts.
# ---------------------------------------------------------------------------
TECH_VOCAB = {
    # ── AI / Machine Learning / LLMs ───────────────────────────────────────
    "ai", "ml", "llm", "rag", "gpt", "nlp", "bert", "lora", "rlhf", "model",
    "agent", "agents", "weights", "inference", "diffusion", "vision",
    "embedding", "transformer", "modelops", "mlops", "swarms", "openai",
    "anthropic", "mistral", "huggingface", "pytorch", "tensorflow", "deeplearning",
    "machine learning", "deep learning", "large language model", "neural network",
    "reinforcement learning", "vector database",

    # ── Programming Languages & Runtimes ───────────────────────────────────
    "python", "rust", "golang", "go", "typescript", "javascript", "kotlin",
    "scala", "swift", "elixir", "haskell", "julia", "lua", "dart", "cpp",
    "c", "wasm", "webassembly", "ebpf", "compiler", "runtime",

    # ── Operating Systems & Systems Architecture ───────────────────────────
    "linux", "unix", "macos", "android", "ios", "windows", "netbsd", "freebsd",
    "ubuntu", "debian", "kernel", "firmware", "scheduler", "distributed systems",
    "system design", "operating system",

    # ── Cloud, DevOps & Infrastructure ─────────────────────────────────────
    "kubernetes", "docker", "terraform", "ansible", "k8s", "aws", "gcp",
    "azure", "serverless", "nginx", "container", "containers", "cloud",
    "microservices", "cloud computing", "open source",

    # ── Databases & Data Engineering ───────────────────────────────────────
    "sql", "nosql", "postgres", "postgresql", "mysql", "sqlite", "kafka",
    "spark", "dbt", "etl", "duckdb", "redis", "elasticsearch", "clickhouse",
    "database", "lakehouse", "warehouse",

    # ── Hardware, Chips & Architecture ─────────────────────────────────────
    "gpu", "cpu", "tpu", "fpga", "arm", "riscv", "nvme", "chip", "semiconductor",
    "blackwell", "lenovo",

    # ── Networking, Protocols & Security ───────────────────────────────────
    "api", "grpc", "graphql", "ssh", "tls", "ssl", "vpn", "dns", "ipv4",
    "ipv6", "tcp", "udp", "http", "https", "security", "encryption", "aes",
    "cve", "cybersecurity", "fingerprint", "zero trust",

    # ── Developer Tools & Web Ecosystem ───────────────────────────────────
    "google", "nvidia", "meta", "apple", "microsoft", "github", "cloudflare",
    "binance", "react", "fastapi", "django", "nodejs", "difftool", "frameworks",
    "xr", "vr", "ar", "blockchain", "crypto", "bitcoin", "ethereum"
}


# ---------------------------------------------------------------------------
# STEP 1: Basic Text Cleaning
# Converts text to lowercase, strips URLs, punctuation, and unwanted symbols.
# ---------------------------------------------------------------------------
def clean_title_text(title: str) -> str:
    """Converts title to lowercase, removes URLs, punctuation, and extra whitespace."""
    if not title or pd.isna(title):
        return ""
    text = str(title).lower()
    # Strip URL links
    text = re.sub(r"https?://\S+|www\.\S+", "", text)
    # Strip punctuation and special characters (keep only alphanumeric and whitespace)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    # Collapse multiple whitespaces
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# STEP 2, 3, 4: POS Tagging + Vocabulary Matching + Generic Filter
# ---------------------------------------------------------------------------
def tokenize_and_filter_stopwords(cleaned_text: str) -> Optional[str]:
    """Extracts verified technical keywords and phrases using a 5-step NLP pipeline:

    Step 1: Input is clean text (from clean_title_text).
    Step 2: POS Tagging - identifies NOUN, PROPN, and technical ADJ candidates.
            Also extracts 2-word compound candidates (ADJ+NOUN or NOUN+NOUN).
    Step 3: Technical Vocabulary - validates candidates against TECH_VOCAB.
    Step 4: Generic Filtering - eliminates non-technical generic terms.
    
    Returns:
      JSON string of tokens if any found, otherwise None (empty cell/NULL in SQLite).
    """
    if not cleaned_text:
        return None

    doc = _nlp(cleaned_text)
    tokens = list(doc)

    extracted_tokens = []
    seen = set()
    consumed_indices = set()

    # Step 2A: Compound Phrases (Adjacent Noun/Adj + Noun pairs or known Tech Phrases)
    for i in range(len(tokens) - 1):
        t1, t2 = tokens[i], tokens[i + 1]
        phrase = f"{t1.text} {t2.text}".strip()

        # Step 3: Validate against Technical Vocabulary
        if phrase in TECH_VOCAB and phrase not in seen:
            extracted_tokens.append(phrase)
            seen.add(phrase)
            consumed_indices.add(i)
            consumed_indices.add(i + 1)

    # Step 2B: Single-Word Candidates (NOUN, PROPN, and select technical ADJ)
    for token in tokens:
        if token.i in consumed_indices:
            continue

        word = token.text.strip()
        lemma = token.lemma_.strip()
        singular = word.rstrip("s") if len(word) > 3 and word.endswith("s") else word

        # Step 4: Drop stopwords and generic non-technical terms
        if (
            not word
            or len(word) < 2
            or word in STOPWORDS
            or word.isdigit()
            or word in GENERIC_TERMS
            or lemma in GENERIC_TERMS
        ):
            continue

        # Step 2: Must be a NOUN, PROPN, or technical candidate
        if token.pos_ not in {"NOUN", "PROPN", "ADJ"}:
            continue

        # Step 3: Must be verified in the Technical Vocabulary
        is_tech = (
            word in TECH_VOCAB
            or lemma in TECH_VOCAB
            or singular in TECH_VOCAB
        )

        if is_tech and word not in seen:
            extracted_tokens.append(word)
            seen.add(word)

    # Return None instead of empty array [] to keep cell empty (NULL) in database
    return json.dumps(extracted_tokens) if extracted_tokens else None


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
