"""
PostgreSQL Session - Remote connection to API server database.

Provides database session for the worker to access return data,
queue events, and all other tables stored in PostgreSQL.
"""
import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session, declarative_base

from config import settings

logger = logging.getLogger(__name__)

# Create SQLAlchemy engine for PostgreSQL
engine = create_engine(
    settings.POSTGRES_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    echo=False
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.
    
    Usage:
        with get_session() as db:
            db.query(...)
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI-style dependency for database sessions.
    
    Usage:
        db: Session = Depends(get_db)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database tables."""
    from models import amazon_return, worker_queue, order_details, statistics, dhl_tracking
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables initialized")


def test_connection() -> bool:
    """Test database connection."""
    try:
        with get_session() as db:
            db.execute(text("SELECT 1"))
        logger.info(f"PostgreSQL connection successful: {settings.POSTGRES_URL.split('@')[1]}")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        return False
