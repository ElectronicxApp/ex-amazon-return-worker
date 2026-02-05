"""
Worker Queue Model - PostgreSQL-based queue for worker triggers.

The API server inserts events, and the worker polls for them.
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, JSON, Text

from db.postgres_session import Base


class WorkerQueueEvent(Base):
    """Queue events for worker processing triggers."""
    __tablename__ = "worker_queue_events"
    
    id = Column(Integer, primary_key=True)
    
    # Event type: PROCESS_RETURNS
    event_type = Column(String(50), nullable=False, index=True)
    
    # Status: PENDING, PROCESSING, COMPLETED, FAILED
    status = Column(String(20), default="PENDING", index=True)
    
    # Event data (e.g., {"days": 90})
    data = Column(JSON, nullable=True)
    
    # Who triggered the event
    triggered_by = Column(String(100), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Error tracking
    error_message = Column(Text, nullable=True)
    
    def __repr__(self):
        return f"<WorkerQueueEvent(id={self.id}, type={self.event_type}, status={self.status})>"
