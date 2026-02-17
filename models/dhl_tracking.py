"""
Carrier Tracking SQLAlchemy models for the worker.

Tables:
- DHLTrackingData: Shipment-level tracking information (one record per tracking_number + return_id pair)
- DHLTrackingEvent: Timeline events (multiple per tracking record)
- DHLTrackingSignature: Proof of delivery signatures / POD URLs

Notes:
- Supports DHL (XML API) and DPD (public REST API) carriers via the `carrier` column.
- DHL reuses tracking numbers after ~90 days. We use a surrogate PK (id) with
  a UNIQUE(tracking_number, return_id) constraint so the same tracking number
  can coexist for different returns.
- tracking_state includes 'no_data' for shipments where the API returned no data
  on the first attempt. These are immediately closed (tracking_stopped_at set).
- For DHL: ice_code/ric_code contain DHL ICE/RIC codes.
- For DPD: ice_code stores DPD statusCode (e.g. '13'), ric_code stores serviceCode.
  standard_event_code stores DPD scanType.name (e.g. 'SC_13_DELIVERED').
- Signatures: DHL stores binary GIF image; DPD stores a pod_url link.
"""

from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime,
    ForeignKey, LargeBinary, Index, CheckConstraint, UniqueConstraint
)
from sqlalchemy.orm import relationship

from db.postgres_session import Base


class DHLTrackingData(Base):
    """Shipment-level tracking data, one record per (tracking_number, return_id) pair."""
    __tablename__ = "dhl_tracking_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tracking_number = Column(String(100), nullable=False, index=True)
    return_id = Column(Integer, ForeignKey("amazon_returns.id", ondelete="CASCADE"), nullable=False, index=True)
    carrier = Column(String(20), nullable=False, default="DHL", index=True)  # DHL, DPD

    # Current status
    current_status = Column(Text, nullable=True)
    current_short_status = Column(String(100), nullable=True)
    current_ice_code = Column(String(50), nullable=True)
    current_ric_code = Column(String(50), nullable=True)
    last_update_timestamp = Column(DateTime, nullable=True)

    # Delivery info
    delivery_flag = Column(Integer, default=0)
    delivery_date = Column(DateTime, nullable=True)

    # Recipient
    recipient_id = Column(String(100), nullable=True)
    recipient_id_text = Column(Text, nullable=True)

    # Product/routing
    product_code = Column(String(50), nullable=True)
    product_name = Column(String(255), nullable=True)
    dest_country = Column(String(100), nullable=True)
    origin_country = Column(String(100), nullable=True)

    # Tracking lifecycle
    tracking_state = Column(String(20), nullable=False, default="active", index=True)
    tracking_started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    tracking_stopped_at = Column(DateTime, nullable=True)

    # Signature retrieval
    signature_retrieved = Column(Boolean, default=False)
    signature_retrieval_failed = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    events = relationship("DHLTrackingEvent", back_populates="tracking_data", cascade="all, delete-orphan", order_by="DHLTrackingEvent.event_timestamp.desc()")
    signature = relationship("DHLTrackingSignature", back_populates="tracking_data", uselist=False, cascade="all, delete-orphan")
    amazon_return = relationship("AmazonReturn", back_populates="tracking_data")

    __table_args__ = (
        UniqueConstraint("tracking_number", "return_id", name="uq_tracking_number_return_id"),
        CheckConstraint(
            "tracking_state IN ('active', 'delivered', 'expired', 'exception', 'no_data')",
            name="ck_tracking_state",
        ),
        Index("ix_dhl_tracking_data_delivery_flag", "delivery_flag"),
    )


class DHLTrackingEvent(Base):
    """Individual tracking event in the shipment timeline."""
    __tablename__ = "dhl_tracking_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tracking_data_id = Column(Integer, ForeignKey("dhl_tracking_data.id", ondelete="CASCADE"), nullable=False, index=True)
    tracking_number = Column(String(100), nullable=False, index=True)  # denormalized for convenience

    # Event info
    event_timestamp = Column(DateTime, nullable=True, index=True)
    event_status = Column(Text, nullable=True)
    event_short_status = Column(String(255), nullable=True)
    ice_code = Column(String(50), nullable=True)
    ric_code = Column(String(50), nullable=True)
    standard_event_code = Column(String(50), nullable=True)
    event_location = Column(String(255), nullable=True)
    event_country = Column(String(100), nullable=True)
    event_sequence = Column(Integer, default=0)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    tracking_data = relationship("DHLTrackingData", back_populates="events")


class DHLTrackingSignature(Base):
    """Proof of delivery: binary signature image (DHL) or POD URL (DPD)."""
    __tablename__ = "dhl_tracking_signatures"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tracking_data_id = Column(Integer, ForeignKey("dhl_tracking_data.id", ondelete="CASCADE"), nullable=False, index=True)
    tracking_number = Column(String(100), nullable=False, index=True)  # denormalized for convenience

    # Signature data (DHL: binary image; DPD: pod_url)
    signature_image = Column(LargeBinary, nullable=True)
    pod_url = Column(Text, nullable=True)  # DPD proof-of-delivery URL
    signature_date = Column(DateTime, nullable=True)
    retrieved_at = Column(DateTime, nullable=True)
    mime_type = Column(String(50), default="image/gif")
    retrieval_failed = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    tracking_data = relationship("DHLTrackingData", back_populates="signature")
