"""
Statistics SQLAlchemy models.

Tables for pre-aggregated dashboard metrics:
- ReturnStatistics (alias for StatisticsGlobal): Single-row global metrics
- MarketplaceStats: Aggregated by marketplace
- ReasonStats: Aggregated by return reason
- TopProduct: Top returned products
"""

from datetime import datetime, date

from sqlalchemy import Column, Integer, String, Text, DateTime, Date, Numeric, Index

from db.postgres_session import Base


# ==================== Marketplace Mapping ====================

MARKETPLACE_COUNTRY_MAP = {
    "A28R8C7NBKEWEA": ("Belgium", "BE"),
    "AMEN7PMS3EDWL": ("Belgium (NL)", "BE"),
    "A17E79C6D8DWNP": ("Sweden", "SE"),
    "A1F83G8C2ARO7P": ("United Kingdom", "GB"),
    "A2VIGQ35RCS4UG": ("Spain", "ES"),
    "APJ6JRA9NG5V4": ("Italy", "IT"),
    "A2NODRKZP88ZB9": ("Sweden", "SE"),
    "A1C3SOZRARQ6R3": ("Poland", "PL"),
    "A33AVAJ2PDY3EV": ("Netherlands", "NL"),
    "A13V1IB3VIYZZH": ("France", "FR"),
    "A1805IZSGTT6HS": ("Netherlands", "NL"),
    "A1RKKUPIHCS9HS": ("Spain", "ES"),
    "A1PA6795UKMFR9": ("Germany", "DE"),
}


# ==================== Reason Display Names ====================

REASON_DISPLAY_MAP = {
    "AMZ-PG-BAD-DESC": "Product Description Inaccurate",
    "AMZ-PG-MISORDERED": "Misordered",
    "CR-DAMAGED_BY_CARRIER": "Damaged by Carrier",
    "CR-DAMAGED_BY_FC": "Damaged by Fulfillment Center",
    "CR-DEFECTIVE": "Defective",
    "CR-EXTRA_ITEM": "Extra Item",
    "CR-FOUND_BETTER_PRICE": "Found Better Price",
    "CR-NO_REASON_GIVEN": "No Reason Given",
    "CR-ORDERED_WRONG_ITEM": "Ordered Wrong Item",
    "CR-QUALITY_UNACCEPTABLE": "Quality Unacceptable",
    "CR-SWITCHEROO": "Switcheroo",
    "CR-UNAUTHORIZED_PURCHASE": "Unauthorized Purchase",
    "CR-UNWANTED_ITEM": "Unwanted Item",
    "CR-NOT_COMPATIBLE": "Not Compatible",
    "EXCESSIVE_INSTALLATION": "Excessive Installation Effort",
    "NEVER_ARRIVED": "Never Arrived",
    "PART_NOT_COMPATIBLE": "Part Not Compatible",
    "DEFECTIVE": "Defective",
    "NOT_AS_DESCRIBED": "Not As Described",
    "WRONG_ITEM": "Wrong Item Received",
    "DAMAGED_DURING_SHIPPING": "Damaged During Shipping",
    "NO_LONGER_NEEDED": "No Longer Needed",
    "ORDERED_WRONG_ITEM": "Ordered Wrong Item",
    "BETTER_PRICE_AVAILABLE": "Better Price Available",
    "OTHER": "Other",
}


class StatisticsDaily(Base):
    """Daily aggregated metrics for trend charts."""
    __tablename__ = "statistics_daily"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stat_date = Column(Date, nullable=False, unique=True, index=True)
    
    # Return Counts
    total_returns = Column(Integer, default=0)
    newly_fetched = Column(Integer, default=0)
    
    # Refund Amounts
    actual_refund_amount = Column(Numeric(14, 2), default=0)
    requested_refund_amount = Column(Numeric(14, 2), default=0)
    
    # Processing Metrics
    labels_created = Column(Integer, default=0)
    labels_submitted = Column(Integer, default=0)
    
    # Internal Status Breakdown
    status_pending_rma = Column(Integer, default=0)
    status_no_rma_found = Column(Integer, default=0)
    status_duplicate_closed = Column(Integer, default=0)
    status_not_eligible = Column(Integer, default=0)
    status_processing_error = Column(Integer, default=0)
    status_completed = Column(Integer, default=0)
    
    # Special Flags Count
    prime_returns = Column(Integer, default=0)
    gift_returns = Column(Integer, default=0)
    ato_z_claims = Column(Integer, default=0)
    
    # Timestamps
    computed_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class StatisticsReason(Base):
    """Aggregated statistics by return reason code."""
    __tablename__ = "statistics_reason"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    reason_code = Column(String(100), nullable=False, unique=True, index=True)
    reason_display = Column(String(255), nullable=True)
    
    # Metrics
    total_count = Column(Integer, default=0)
    total_value = Column(Numeric(14, 2), default=0)
    avg_value = Column(Numeric(10, 2), default=0)
    
    # Timestamps
    computed_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class StatisticsMarketplace(Base):
    """Aggregated statistics by Amazon marketplace."""
    __tablename__ = "statistics_marketplace"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    marketplace_id = Column(String(50), nullable=False, unique=True, index=True)
    marketplace_name = Column(String(100), nullable=True)
    country_code = Column(String(10), nullable=True)
    
    # Metrics
    total_count = Column(Integer, default=0)
    total_value = Column(Numeric(14, 2), default=0)
    avg_value = Column(Numeric(10, 2), default=0)
    
    # Timestamps
    computed_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class StatisticsTopProducts(Base):
    """Top returned products for analysis."""
    __tablename__ = "statistics_top_products"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    asin = Column(String(20), nullable=False, unique=True, index=True)
    product_title = Column(Text, nullable=True)
    product_image_url = Column(Text, nullable=True)
    merchant_sku = Column(String(100), nullable=True)
    
    # Metrics
    return_count = Column(Integer, default=0, index=True)
    total_value = Column(Numeric(14, 2), default=0)
    
    # Top reason for this product
    top_reason_code = Column(String(100), nullable=True)
    
    # Ranking
    rank = Column(Integer, nullable=True, index=True)
    
    # Timestamps
    computed_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class StatisticsGlobal(Base):
    """Single-row global metrics snapshot."""
    __tablename__ = "statistics_global"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Returns (today, week, all-time)
    returns_today = Column(Integer, default=0)
    returns_this_week = Column(Integer, default=0)
    returns_all_time = Column(Integer, default=0)
    
    # Actual Refunds (filtered)
    actual_refund_today = Column(Numeric(12, 2), default=0)
    actual_refund_this_week = Column(Numeric(12, 2), default=0)
    actual_refund_all_time = Column(Numeric(14, 2), default=0)
    
    # Requested Refunds (unfiltered)
    requested_refund_today = Column(Numeric(12, 2), default=0)
    requested_refund_this_week = Column(Numeric(12, 2), default=0)
    requested_refund_all_time = Column(Numeric(14, 2), default=0)
    
    # Labels Created
    labels_created_today = Column(Integer, default=0)
    labels_created_this_week = Column(Integer, default=0)
    labels_created_all_time = Column(Integer, default=0)
    
    # Newly Fetched Returns
    newly_fetched_today = Column(Integer, default=0)
    newly_fetched_this_week = Column(Integer, default=0)
    newly_fetched_all_time = Column(Integer, default=0)
    
    # Internal Status Breakdown
    status_pending_rma = Column(Integer, default=0)
    status_no_rma_found = Column(Integer, default=0)
    status_duplicate_closed = Column(Integer, default=0)
    status_not_eligible = Column(Integer, default=0)
    status_processing_error = Column(Integer, default=0)
    status_completed = Column(Integer, default=0)
    
    # Special Flags
    prime_returns_all_time = Column(Integer, default=0)
    ato_z_claims_all_time = Column(Integer, default=0)
    gift_returns_all_time = Column(Integer, default=0)
    
    # Median Values
    median_returns = Column(Numeric(10, 2), default=0)
    median_requested_refunds = Column(Numeric(14, 2), default=0)
    
    # Timestamps
    computed_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# Type aliases for cleaner imports
ReturnStatistics = StatisticsGlobal
MarketplaceStats = StatisticsMarketplace
ReasonStats = StatisticsReason
TopProduct = StatisticsTopProducts
