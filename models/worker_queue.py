"""
Worker Queue Model - PostgreSQL-based queue for worker triggers.

The API server inserts events, and the worker polls for them.
Includes step-level progress tracking for real-time status updates.
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, JSON, Text

from db.postgres_session import Base


# Step definitions for progress tracking
PROCESSING_STEPS = [
    ("session_init", "Initializing Session"),
    ("fetch_returns", "Fetching Returns"),
    ("fetch_addresses", "Fetching Addresses"),
    ("rma_lookup", "Looking up RMAs"),
    ("status_check", "Checking Status"),
    ("detect_duplicates", "Detecting Duplicates"),
    ("filter_eligible", "Filtering Eligible"),
    ("generate_labels", "Generating Labels"),
    ("upload_labels", "Uploading Labels"),
    ("aggregation", "Updating Statistics"),
]

TOTAL_STEPS = len(PROCESSING_STEPS)


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
    
    # Result data (JSON) - populated on completion
    result = Column(JSON, nullable=True)
    
    # Who triggered the event
    triggered_by = Column(String(100), nullable=True)
    
    # === Progress Tracking ===
    # Current step being executed
    current_step = Column(String(50), nullable=True)
    
    # Current step index (0-9 for 10 total steps)
    current_step_index = Column(Integer, default=0)
    
    # Total number of steps (default 10)
    total_steps = Column(Integer, default=TOTAL_STEPS)
    
    # Per-step details as JSON
    # Example: {"fetch_returns": {"total": 100, "new": 5}, "rma_lookup": {"found": 10, "not_found": 2}}
    step_progress = Column(JSON, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Error tracking
    error_message = Column(Text, nullable=True)
    
    @property
    def percent_complete(self) -> float:
        """Calculate percentage complete based on current step."""
        if self.status == "COMPLETED":
            return 100.0
        if self.status == "PENDING":
            return 0.0
        if self.current_step_index is None:
            return 0.0
        return round((self.current_step_index / self.total_steps) * 100, 1)
    
    def __repr__(self):
        return f"<WorkerQueueEvent(id={self.id}, type={self.event_type}, status={self.status})>"
