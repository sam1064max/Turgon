"""
Shared utilities: date parsing, hashing, logging, DB connection.
"""

import hashlib
import logging
import re
import sys
from datetime import datetime, timezone

import pandas as pd
from dateutil import parser as dateutil_parser
from sqlalchemy import create_engine, text

from config.settings import settings


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    """Create a configured logger with consistent formatting."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
    return logger


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_engine = None


def get_engine():
    """Singleton SQLAlchemy engine with fail-safe error handling."""
    global _engine
    if _engine is None:
        try:
            _engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
        except Exception as e:
            get_logger("utils").warning(f"Could not initialize database engine: {e}")
            return None
    return _engine


def execute_sql(sql: str, params: dict = None):
    """Execute raw SQL statement safely."""
    engine = get_engine()
    if engine is None:
        return None
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            conn.commit()
            return result
    except Exception as e:
        get_logger("utils").warning(f"Execute SQL failed: {e}")
        return None


def read_sql(sql: str, params: dict = None) -> pd.DataFrame:
    """Read SQL query into a DataFrame safely."""
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception as e:
        get_logger("utils").warning(f"Read SQL failed: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------

def compute_row_hash(row: pd.Series) -> str:
    """Compute SHA-256 hash of all values in a row for dedup tracking."""
    raw = "|".join(str(v) for v in row.values)
    return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()


# ---------------------------------------------------------------------------
# Date Parsing
# ---------------------------------------------------------------------------

# Pre-compiled pattern for Unix epoch timestamps (10-digit integers)
_EPOCH_RE = re.compile(r"^\d{10}$")


def parse_date(value) -> datetime | None:
    """
    Parse a date string in any of the ~6 formats found in the dataset:
      - ISO 8601: 2024-10-24 14:32:10, 2024-07-25T18:27:44
      - US format: 02/03/2025, 02/14/2025 08:16 AM
      - European text: 28-Feb-2024 17:03, 12-Feb-2025 03:21
      - Date only: 2025-07-01, 04/09/2025
      - Unix epoch: 1726547768 (10-digit integer)
      - None/NaN: returns None

    Returns a timezone-naive datetime or None.
    """
    if pd.isna(value) or value is None:
        return None

    s = str(value).strip()
    if not s or s.lower() in ("", "nan", "none", "null", "n/a"):
        return None

    # Unix epoch (10-digit integer)
    if _EPOCH_RE.match(s):
        try:
            return datetime.fromtimestamp(int(s), tz=timezone.utc).replace(tzinfo=None)
        except (ValueError, OSError):
            return None

    # General-purpose parser (handles ISO, US, European text, AM/PM)
    try:
        dt = dateutil_parser.parse(s, dayfirst=False, fuzzy=True)
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except (ValueError, TypeError, OverflowError):
        return None


# ---------------------------------------------------------------------------
# Category Normalization
# ---------------------------------------------------------------------------

# Canonical categories and their known raw-value mappings
CATEGORY_MAP = {
    # HVAC
    "hvac": "HVAC", "a/c": "HVAC", "ac": "HVAC", "hvac system": "HVAC",
    "air conditioning": "HVAC", "heating/cooling": "HVAC",
    "heating cooling": "HVAC", "climate control": "HVAC",
    # Electrical
    "electrical": "Electrical", "electrical systems": "Electrical",
    "elec": "Electrical", "power": "Electrical", "power issue": "Electrical",
    # Plumbing
    "plumbing": "Plumbing", "plumbing issue": "Plumbing",
    "water/plumbing": "Plumbing", "water issue": "Plumbing",
    # Fire Safety
    "fire safety": "Fire Safety", "fire/safety": "Fire Safety",
    "fire alarm": "Fire Safety", "sprinkler": "Fire Safety",
    # Elevator
    "elevator": "Elevator", "elevator/escalator": "Elevator",
    "vertical transport": "Elevator", "lift": "Elevator",
    # Security
    "security": "Security", "security systems": "Security",
    "badge/access": "Security", "access control": "Security",
    # Cleaning/Janitorial
    "cleaning": "Cleaning", "janitorial": "Cleaning",
    "housekeeping": "Cleaning",
    # IT/Network
    "it/network": "IT/Network", "network": "IT/Network",
    "connectivity": "IT/Network", "it": "IT/Network",
    "wifi": "IT/Network", "it support": "IT/Network",
    # Pest Control
    "pest control": "Pest Control", "pest": "Pest Control",
    "exterminator": "Pest Control",
    # General/Other
    "other": "General", "misc": "General", "general": "General",
    "general maintenance": "General", "maintenance": "General",
}


def normalize_category(raw: str | None) -> str | None:
    """Map a raw category string to a canonical category."""
    if pd.isna(raw) or raw is None:
        return None
    cleaned = str(raw).strip().lower()
    if cleaned in ("", "unknown", "???", "delete me", "test", "asdf"):
        return None
    # Direct lookup
    if cleaned in CATEGORY_MAP:
        return CATEGORY_MAP[cleaned]
    # Check if it's a description accidentally in the category field
    if len(cleaned) > 50:
        return None  # Will be handled by the semantic classifier or fallback
    # Fuzzy fallback: check if any key is a substring
    for key, canonical in CATEGORY_MAP.items():
        if key in cleaned or cleaned in key:
            return canonical
    return "General"


# ---------------------------------------------------------------------------
# Priority Normalization
# ---------------------------------------------------------------------------

PRIORITY_MAP = {
    "critical": "Critical", "crit": "Critical", "urgent!!!": "Critical",
    "high": "High", "hi": "High",
    "medium": "Medium", "med": "Medium", "normal": "Medium",
    "low": "Low", "lo": "Low",
}


def normalize_priority(raw: str | None) -> str | None:
    """Map a raw priority string to Critical/High/Medium/Low."""
    if pd.isna(raw) or raw is None:
        return None
    cleaned = str(raw).strip().lower()
    if cleaned in ("", "???", "unknown"):
        return None
    return PRIORITY_MAP.get(cleaned)


# ---------------------------------------------------------------------------
# Cost Cleaning
# ---------------------------------------------------------------------------

def clean_cost(raw) -> float | None:
    """
    Clean cost values:
      - Strip $ prefix
      - TBD, error, N/A → None
      - Negative values (-1 sentinel) → None
    """
    if pd.isna(raw) or raw is None:
        return None
    s = str(raw).strip()
    if s.lower() in ("", "tbd", "error", "n/a", "nan", "none"):
        return None
    s = s.lstrip("$").strip()
    try:
        val = float(s)
        return None if val < 0 else val
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# SLA Cleaning
# ---------------------------------------------------------------------------

def clean_sla(raw) -> int | None:
    """
    Clean SLA hours:
      - -1, 0, 999 are sentinel/placeholder values → None
      - N/A → None
      - Valid values: 4, 8, 24, 48, 72
    """
    if pd.isna(raw) or raw is None:
        return None
    try:
        val = int(float(raw))
    except (ValueError, TypeError):
        return None
    if val in (-1, 0, 999):
        return None
    if val < 0:
        return None
    return val


# ---------------------------------------------------------------------------
# Submitter Name Normalization
# ---------------------------------------------------------------------------

# Build a mapping from all known variants to canonical names
_NAME_VARIANTS = {
    # John Smith
    "john smith": "John Smith", "j. smith": "John Smith",
    # Jane Doe
    "jane doe": "Jane Doe", "j. doe": "Jane Doe",
    # David Kim
    "david kim": "David Kim", "d. kim": "David Kim",
    # Lisa Chen
    "lisa chen": "Lisa Chen", "l. chen": "Lisa Chen",
    # Robert Martinez / Bob Martinez
    "robert martinez": "Robert Martinez", "r. martinez": "Robert Martinez",
    "bob martinez": "Robert Martinez",
    # Michael Johnson / Mike Johnson
    "michael johnson": "Michael Johnson", "m. johnson": "Michael Johnson",
    "mike johnson": "Michael Johnson",
    # Chris Taylor
    "chris taylor": "Chris Taylor", "c. taylor": "Chris Taylor",
    # Sarah Williams
    "sarah williams": "Sarah Williams", "s. williams": "Sarah Williams",
    # Emily Brown
    "emily brown": "Emily Brown", "e. brown": "Emily Brown",
    # Pat Anderson
    "pat anderson": "Pat Anderson",
    # Jordan Rivera
    "jordan rivera": "Jordan Rivera",
    # Sam Jackson
    "sam jackson": "Sam Jackson",
    # Karen Lee
    "karen lee": "Karen Lee", "k. lee": "Karen Lee",
    # Tom Wilson
    "tom wilson": "Tom Wilson", "t. wilson": "Tom Wilson",
    # Alex Garcia
    "alex garcia": "Alex Garcia", "a. garcia": "Alex Garcia",
    # Nancy White
    "nancy white": "Nancy White",
}


def normalize_submitter(raw: str | None) -> str | None:
    """Normalize submitter names to canonical form."""
    if pd.isna(raw) or raw is None:
        return None
    cleaned = str(raw).strip().lower()
    if cleaned in ("", "test", "admin", "system"):
        return None  # Junk submitters
    return _NAME_VARIANTS.get(cleaned, str(raw).strip().title())


# ---------------------------------------------------------------------------
# Status / Building Cleaning
# ---------------------------------------------------------------------------

def clean_status(raw: str | None) -> str | None:
    """Clean status values: unknown/??? → None."""
    if pd.isna(raw) or raw is None:
        return None
    s = str(raw).strip()
    if s.lower() in ("", "unknown", "???"):
        return None
    return s


def clean_building(raw: str | None) -> str | None:
    """Clean building values: unknown/??? → None."""
    if pd.isna(raw) or raw is None:
        return None
    s = str(raw).strip()
    if s.lower() in ("", "unknown", "???"):
        return None
    return s


# ---------------------------------------------------------------------------
# Encoding Fix
# ---------------------------------------------------------------------------

def fix_encoding(text: str | None) -> str | None:
    """Fix mojibake/encoding artifacts (UTF-8 misinterpreted as Latin-1)."""
    if pd.isna(text) or text is None:
        return None
    s = str(text)
    # Common UTF-8 misinterpretation patterns (using escape sequences)
    replacements = {
        "\u00e2\u0080\u0093": "\u2013",  # en-dash
        "\u00e2\u0080\u0094": "\u2014",  # em-dash
        "\u00e2\u0080\u0099": "\u2019",  # right single quote
        "\u00e2\u0080\u009c": "\u201c",  # left double quote
        "\u00e2\u0080\u009d": "\u201d",  # right double quote
        "\x00": "",                       # null bytes
    }
    for bad, good in replacements.items():
        s = s.replace(bad, good)
    # Clean up any remaining mojibake pattern: \u00e2\u0080 + any char
    s = re.sub(r'\u00e2\u0080.', '\u2014', s)
    return s.strip()
