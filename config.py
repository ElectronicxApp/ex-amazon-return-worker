"""
Configuration for Amazon Return Worker.

Loads settings from environment variables.
"""
import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

# Root directory of worker
ROOT_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    """Worker configuration settings."""
    
    # PostgreSQL (Remote - API Server)
    POSTGRES_URL: str = os.getenv(
        "POSTGRES_URL", 
        "postgresql://amazon_returns:password@localhost:5432/amazon_returns"
    )
    
    # JTL SQL Server (Local)
    JTL_SQL_SERVER: str = os.getenv("JTL_SQL_SERVER", r"WIN-LAA4ORO4RU4\JTLWAWI")
    JTL_SQL_DATABASE: str = os.getenv("JTL_SQL_DATABASE", "eazybusiness")
    JTL_SQL_USERNAME: str = os.getenv("JTL_SQL_USERNAME", "reportsuser")
    JTL_SQL_PASSWORD: str = os.getenv("JTL_SQL_PASSWORD", "")
    
    # AWS S3
    AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION: str = os.getenv("AWS_REGION", "eu-central-1")
    S3_BUCKET_NAME: Optional[str] = os.getenv("S3_BUCKET_NAME")
    
    # DHL API
    DHL_USERNAME: Optional[str] = os.getenv("DHL_USERNAME")
    DHL_PASSWORD: Optional[str] = os.getenv("DHL_PASSWORD")
    DHL_CLIENT_ID: Optional[str] = os.getenv("DHL_CLIENT_ID")
    DHL_CLIENT_SECRET: Optional[str] = os.getenv("DHL_CLIENT_SECRET")
    
    # Amazon Credentials
    AMAZON_EMAIL: Optional[str] = os.getenv("AMAZON_EMAIL")
    AMAZON_PASSWORD: Optional[str] = os.getenv("AMAZON_PASSWORD")
    AMAZON_2FA_SECRET: Optional[str] = os.getenv("AMAZON_2FA_SECRET")
    
    # Browser Profile and Cookie Paths (inside ./Browser folder)
    BROWSER_DIR: str = str(ROOT_DIR / "Browser")
    BROWSER_PROFILE_DIR: str = str(ROOT_DIR / "Browser" / "profile")
    AMAZON_COOKIE_FILE: str = str(ROOT_DIR / "Browser" / "amazonCookies.pkl")
    
    # Worker Settings
    WORKER_POLL_INTERVAL: int = int(os.getenv("WORKER_POLL_INTERVAL", "10"))
    WORKER_DAYS_BACK: int = int(os.getenv("WORKER_DAYS_BACK", "90"))
    
    # Retry Settings
    RETRY_MAX_ATTEMPTS: int = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
    RETRY_WAIT_SECONDS: int = int(os.getenv("RETRY_WAIT_SECONDS", "60"))
    RETRY_BACKOFF_MULTIPLIER: int = int(os.getenv("RETRY_BACKOFF_MULTIPLIER", "2"))

    # Circuit Breaker Settings
    CIRCUIT_BREAKER_THRESHOLD: int = int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "5"))
    CIRCUIT_BREAKER_RESET_TIMEOUT: int = int(os.getenv("CIRCUIT_BREAKER_RESET_TIMEOUT", "300"))
    
    # Schedule Settings (comma-separated times in HH:MM format)
    # Default: Run at 6:00 AM, 12:00 PM, and 6:00 PM
    WORKER_SCHEDULE_TIMES: str = os.getenv("WORKER_SCHEDULE_TIMES", "06:00,12:00,18:00")
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
