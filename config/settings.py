"""
Configuration management for the Turgon pipeline.
Loads from environment variables with sensible defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Resolve project root (two levels up from this file: config/ -> project root)
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Settings:
    # Project root directory (absolute)
    BASE_DIR: str = _BASE_DIR

    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql://turgon:turgon_dev@localhost:5432/turgon"
    )

    # Data paths
    RAW_DATA_PATH: str = os.getenv("RAW_DATA_PATH", "data/raw_tickets.csv")
    DATA_FILE_PATH: str = os.getenv(
        "DATA_FILE_PATH",
        os.path.join(_BASE_DIR, "data", "raw_tickets.csv")
    )

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # OpenAI
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    # Agent settings
    AGENT_SAMPLE_SIZE: int = int(os.getenv("AGENT_SAMPLE_SIZE", "200"))
    AGENT_BATCH_SIZE: int = int(os.getenv("AGENT_BATCH_SIZE", "25"))


settings = Settings()
