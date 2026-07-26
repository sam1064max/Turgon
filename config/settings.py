"""
Configuration management for the Turgon pipeline.
Loads from environment variables with sensible defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql://turgon:turgon_dev@localhost:5432/turgon"
    )

    # Data paths
    RAW_DATA_PATH: str = os.getenv("RAW_DATA_PATH", "data/raw_tickets.csv")

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # OpenAI
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    # Agent settings
    AGENT_SAMPLE_SIZE: int = int(os.getenv("AGENT_SAMPLE_SIZE", "200"))
    AGENT_BATCH_SIZE: int = int(os.getenv("AGENT_BATCH_SIZE", "25"))


settings = Settings()
