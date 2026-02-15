"""
Amazon Return SQLAlchemy models.

Core tables:
- AmazonReturn: Main return entity
- AmazonReturnItem: Items in each return
- AmazonReturnAddress: Customer shipping address
- AmazonReturnLabel: Amazon's label details
- AmazonReturnGeneratedLabel: Our DHL-generated labels

Status Flow:
PENDING_RMA → RMA_RECEIVED → ELIGIBLE → LABEL_GENERATED → LABEL_UPLOADED → LABEL_SUBMITTED → COMPLETED
     ↓              ↓              ↓
NO_RMA_FOUND  DUPLICATE_CLOSED  ALREADY_LABEL_SUBMITTED (Amazon already has tracking)
     ↓              
NOT_ELIGIBLE  PROCESSING_ERROR (last_error tracks failure reasons)
"""

import hashlib
import json
from datetime import datetime
from typing import Optional, List

from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime, 
    Numeric, ForeignKey, JSON, Index
)
from sqlalchemy.orm import relationship

from db.postgres_session import Base


class InternalStatus:
    """Internal processing status values."""
    PENDING_RMA = "PENDING_RMA"
    NO_RMA_FOUND = "NO_RMA_FOUND"
    RMA_RECEIVED = "RMA_RECEIVED"
    DUPLICATE_CLOSED = "DUPLICATE_CLOSED"
    NOT_ELIGIBLE = "NOT_ELIGIBLE"
    ALREADY_LABEL_SUBMITTED = "ALREADY_LABEL_SUBMITTED"
    ELIGIBLE = "ELIGIBLE"
    LABEL_GENERATED = "LABEL_GENERATED"
    LABEL_UPLOADED = "LABEL_UPLOADED"
    LABEL_SUBMITTED = "LABEL_SUBMITTED"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    COMPLETED = "COMPLETED"


class ReturnRequestState:
    """Amazon return request state values."""
    COMPLETED = "Completed"
    PENDING_LABEL = "PendingLabel"
    PENDING_REFUND = "PendingRefund"
    PENDING_APPROVAL = "PendingApproval"
    CLOSED = "Closed"


class Resolution:
    """Return resolution types."""
    STANDARD_REFUND = "StandardRefund"
    REFUND_ON_FIRST_SCAN = "RefundOnFirstScan"
    RETURNLESS_REFUND = "ReturnlessRefund"
    AUTOMATED_REFUND = "AutomatedRefund"
    RETURN_ONLY = "ReturnOnly"


class LabelState:
    """Generated label state values."""
    CREATED = "CREATED"
    UPLOADED = "UPLOADED"
    SUBMITTED = "SUBMITTED"
    ERROR = "ERROR"


class AmazonReturn(Base):
    """Main Amazon return entity."""
    __tablename__ = "amazon_returns"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Amazon Identifiers
    return_request_id = Column(String(100), nullable=False, unique=True, index=True)
    order_id = Column(String(50), nullable=False, index=True)
    rma_id = Column(String(50), nullable=True, index=True)
    internal_rma = Column(String(50), nullable=True, index=True)
    marketplace_id = Column(String(50), nullable=True, index=True)
    
    # Internal Processing Status
    internal_status = Column(String(50), nullable=False, default=InternalStatus.PENDING_RMA, index=True)
    last_error = Column(Text, nullable=True)
    
    # Amazon Return State
    return_request_state = Column(String(50), nullable=True, index=True)
    
    # Order & Value Information
    total_order_value = Column(Numeric(12, 2), nullable=True)
    currency_code = Column(String(10), default="EUR")
    customer_id = Column(String(100), nullable=True)
    customer_name = Column(String(255), nullable=True)
    
    # Dates
    order_date = Column(DateTime, nullable=True)
    return_request_date = Column(DateTime, nullable=True, index=True)
    approve_date = Column(DateTime, nullable=True)
    close_date = Column(DateTime, nullable=True)
    
    # Return Details
    in_policy = Column(Boolean, default=True)
    contains_replacement = Column(Boolean, default=False)
    contains_exchange = Column(Boolean, default=False)
    sales_channel = Column(String(50), nullable=True)
    refund_status = Column(String(50), nullable=True)
    
    # Address References
    return_address_id = Column(String(100), nullable=True)
    shipping_address_id = Column(String(100), nullable=True)
    
    # Flags
    prime_return = Column(Boolean, default=False)
    gift_return = Column(Boolean, default=False)
    prp_address = Column(Boolean, default=False)
    ooc_return = Column(Boolean, default=False)
    prime = Column(Boolean, default=False)
    ato_z_claim_filed = Column(Boolean, default=False)
    auto_authorized = Column(Boolean, default=False)
    iba_order = Column(Boolean, default=False)
    replacement_order = Column(Boolean, default=False)
    cosworth_order = Column(Boolean, default=False)
    has_prior_refund = Column(Boolean, default=False)
    
    # Raw Data Storage
    raw_data = Column(JSON, nullable=True)
    data_hash = Column(String(64), nullable=True)
    
    # Label Submission Tracking
    label_submitted_at = Column(DateTime, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    items = relationship("AmazonReturnItem", back_populates="amazon_return", cascade="all, delete-orphan")
    address = relationship("AmazonReturnAddress", back_populates="amazon_return", uselist=False, cascade="all, delete-orphan")
    amazon_label = relationship("AmazonReturnLabel", back_populates="amazon_return", uselist=False, cascade="all, delete-orphan")
    generated_label = relationship("AmazonReturnGeneratedLabel", back_populates="amazon_return", uselist=False, cascade="all, delete-orphan")
    tracking_data = relationship("DHLTrackingData", back_populates="amazon_return", uselist=False, cascade="all, delete-orphan")
    
    def compute_hash(self) -> str:
        """Compute MD5 hash of raw_data for change detection."""
        if self.raw_data:
            data_str = json.dumps(self.raw_data, sort_keys=True)
            return hashlib.md5(data_str.encode()).hexdigest()
        return ""
    
    def mark_error(self, error_message: str):
        """Mark return with error message."""
        self.last_error = error_message
        self.internal_status = InternalStatus.PROCESSING_ERROR


class AmazonReturnItem(Base):
    """Items in each return request."""
    __tablename__ = "amazon_return_items"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    return_id = Column(Integer, ForeignKey("amazon_returns.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Item Identification
    asin = Column(String(20), nullable=False, index=True)
    merchant_sku = Column(String(100), nullable=True)
    order_item_id = Column(String(50), nullable=True)
    
    # Product Details
    product_title = Column(Text, nullable=True)
    product_link = Column(Text, nullable=True)
    product_image_link = Column(Text, nullable=True)
    
    # Return Details
    return_quantity = Column(Integer, default=1)
    return_reason_code = Column(String(100), nullable=True, index=True)
    return_reason_string_id = Column(String(100), nullable=True)
    customer_comments = Column(Text, nullable=True)
    
    # Resolution
    resolution = Column(String(50), nullable=True)
    replacement_order_id = Column(String(50), nullable=True)
    
    # Price
    unit_price = Column(Numeric(10, 2), nullable=True)
    calculated_price = Column(Numeric(10, 2), nullable=True)
    item_condition_type = Column(String(50), nullable=True)
    
    # Flags
    in_policy = Column(Boolean, default=True)
    hazmat_class = Column(String(10), nullable=True)
    is_item_recalled = Column(Boolean, default=False)
    recalled_by = Column(String(100), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    amazon_return = relationship("AmazonReturn", back_populates="items")


class AmazonReturnAddress(Base):
    """Customer shipping address (from routingDetails API)."""
    __tablename__ = "amazon_return_addresses"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    return_id = Column(Integer, ForeignKey("amazon_returns.id", ondelete="CASCADE"), nullable=False, unique=True)
    
    # Address Details
    name = Column(String(255), nullable=True)
    address_field_one = Column(String(500), nullable=True)
    address_field_two = Column(String(500), nullable=True)
    address_field_three = Column(String(500), nullable=True)
    district_or_county = Column(String(100), nullable=True)
    city = Column(String(100), nullable=True)
    state_or_region = Column(String(100), nullable=True)
    postal_code = Column(String(20), nullable=True)
    country_code = Column(String(10), default="DE", index=True)
    phone_number = Column(String(50), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    amazon_return = relationship("AmazonReturn", back_populates="address")


class AmazonReturnLabel(Base):
    """Amazon's own label details (if they provided one)."""
    __tablename__ = "amazon_return_labels"
    
    id = Column(Integer, primary_key=True, index=True)
    return_id = Column(Integer, ForeignKey("amazon_returns.id", ondelete="CASCADE"), nullable=False)
    
    # Label details
    label_type = Column(String(50))
    carrier_name = Column(String(50))
    carrier_tracking_id = Column(String(100))
    label_price = Column(Numeric(10, 2))
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    amazon_return = relationship("AmazonReturn", back_populates="amazon_label")


class AmazonReturnGeneratedLabel(Base):
    """Our DHL-generated labels."""
    __tablename__ = "amazon_return_generated_labels"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    return_id = Column(Integer, ForeignKey("amazon_returns.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # DHL Label Info
    tracking_number = Column(String(100), nullable=False, index=True)
    
    # S3 Storage
    s3_key = Column(String(500), nullable=True)
    
    # Label State
    state = Column(String(50), default=LabelState.CREATED, index=True)
    
    # Timestamps
    submitted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    amazon_return = relationship("AmazonReturn", back_populates="generated_label")


# Create additional indexes
Index("ix_returns_created_at", AmazonReturn.created_at)
Index("ix_items_asin", AmazonReturnItem.asin)
Index("ix_gen_labels_state", AmazonReturnGeneratedLabel.state)
