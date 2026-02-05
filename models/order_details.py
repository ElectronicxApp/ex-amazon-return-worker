"""
Order Details SQLAlchemy models.

Stores order-related data from JTL database linked to AmazonReturn:
- OrderGeneralDetails: Basic order info (buyer, status, items)
- OrderProductDescription: Product descriptions, manufacturer, identifiers
- OrderProductSpec: Product specifications (one row per spec)
- OrderProductAttribute: Product attributes (can be large HTML)
- OrderTrackingInfo: Shipping/tracking information
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship

from db.postgres_session import Base


class OrderGeneralDetails(Base):
    """Basic order info from JTL."""
    __tablename__ = "order_general_details"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    return_id = Column(Integer, ForeignKey("amazon_returns.id"), nullable=False, index=True)
    
    # Order identification
    jtl_order_id = Column(String(50), nullable=True, index=True)
    internal_order_number = Column(String(50), nullable=True)
    
    # Order dates and status
    purchase_date = Column(DateTime, nullable=True)
    status_code = Column(Integer, nullable=True)
    order_status = Column(String(50), nullable=True)
    
    # Order flags
    is_fba = Column(Boolean, default=False)
    is_prime = Column(Boolean, default=False)
    
    # Buyer info
    buyer_name = Column(String(200), nullable=True)
    buyer_email = Column(String(200), nullable=True)
    
    # Product info
    sku = Column(String(100), nullable=True, index=True)
    product_name = Column(Text, nullable=True)
    asin = Column(String(20), nullable=True, index=True)
    quantity_purchased = Column(Integer, default=1)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index("ix_order_general_return_sku", "return_id", "sku"),
    )


class OrderProductDescription(Base):
    """Product descriptions, manufacturer info, and identifiers from JTL."""
    __tablename__ = "order_product_descriptions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    return_id = Column(Integer, ForeignKey("amazon_returns.id"), nullable=False, index=True)
    
    # Product identification
    sku = Column(String(100), nullable=True, index=True)
    internal_id = Column(Integer, nullable=True)
    
    # Names and descriptions
    display_name = Column(String(500), nullable=True)
    description_html = Column(Text, nullable=True)
    short_description = Column(Text, nullable=True)
    
    # Manufacturer and identifiers
    manufacturer = Column(String(200), nullable=True)
    ean = Column(String(50), nullable=True)
    mpn = Column(String(100), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index("ix_order_desc_return_sku", "return_id", "sku"),
    )


class OrderProductSpec(Base):
    """Product specifications (Merkmale) from JTL."""
    __tablename__ = "order_product_specs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    return_id = Column(Integer, ForeignKey("amazon_returns.id"), nullable=False, index=True)
    
    # Spec info
    sku = Column(String(100), nullable=True, index=True)
    spec_name = Column(String(200), nullable=True)
    spec_value = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index("ix_order_spec_return_sku", "return_id", "sku"),
    )


class OrderProductAttribute(Base):
    """Product attributes from JTL."""
    __tablename__ = "order_product_attributes"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    return_id = Column(Integer, ForeignKey("amazon_returns.id"), nullable=False, index=True)
    
    # Attribute info
    sku = Column(String(100), nullable=True, index=True)
    attribute_name = Column(String(200), nullable=True)
    attribute_value = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index("ix_order_attr_return_sku", "return_id", "sku"),
    )


class OrderTrackingInfo(Base):
    """Shipping/tracking information from JTL."""
    __tablename__ = "order_tracking_info"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    return_id = Column(Integer, ForeignKey("amazon_returns.id"), nullable=False, index=True)
    
    # Tracking info
    tracking_number = Column(String(100), nullable=True, index=True)
    carrier_id = Column(Integer, nullable=True)
    
    # Dates
    shipped_date = Column(DateTime, nullable=True)
    delivery_note_date = Column(DateTime, nullable=True)
    
    # Order reference
    internal_order_number = Column(String(50), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index("ix_order_tracking_return", "return_id"),
    )
