"""
Fetch Service - Ingests returns from Amazon and stores in database.

Handles:
- Fetching from Amazon API (via browser automation)
- Detecting new vs existing returns
- Computing data hash for change detection
- Creating items, storing raw data
"""

import hashlib
import json
import logging
from datetime import datetime
from typing import List, Tuple, Optional

from sqlalchemy.orm import Session

from models.amazon_return import (
    AmazonReturn, AmazonReturnItem, AmazonReturnLabel,
    InternalStatus, Resolution
)

logger = logging.getLogger(__name__)


class FetchService:
    """Service for fetching and ingesting Amazon returns."""
    
    def __init__(self, db: Session, amazon_client):
        self.db = db
        self.client = amazon_client
        
    def compute_hash(self, data: dict) -> str:
        """Compute MD5 hash for change detection."""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
        
    def parse_datetime(self, timestamp: Optional[int]) -> Optional[datetime]:
        """Parse Amazon epoch timestamp to datetime."""
        if timestamp:
            try:
                return datetime.fromtimestamp(timestamp / 1000)
            except:
                pass
        return None
        
    def parse_item(self, item_data: dict, return_id: int, calculated_price: float) -> AmazonReturnItem:
        """Parse a return item from API response."""
        unit_price = float(item_data.get("unitPrice") or 0)
        
        return AmazonReturnItem(
            return_id=return_id,
            asin=item_data.get("asin", ""),
            merchant_sku=item_data.get("merchantSKU"),
            order_item_id=item_data.get("orderItemId"),
            product_title=item_data.get("productTitle"),
            product_link=item_data.get("productLink"),
            product_image_link=item_data.get("productImageLink"),
            return_quantity=item_data.get("returnQuantity", 1),
            return_reason_code=item_data.get("returnReasonCode"),
            return_reason_string_id=item_data.get("returnReasonStringId"),
            customer_comments=item_data.get("customerComments"),
            resolution=item_data.get("resolution"),
            replacement_order_id=item_data.get("replacementOrderId"),
            unit_price=unit_price if unit_price > 0 else None,
            calculated_price=calculated_price,
            item_condition_type=item_data.get("itemConditionTypeStringId"),
            in_policy=item_data.get("inPolicy", True),
            hazmat_class=item_data.get("hazmatClass"),
            is_item_recalled=item_data.get("isItemRecalled", False),
            recalled_by=item_data.get("recalledBy"),
        )
        
    def parse_label_details(self, label_data: dict, return_id: int) -> AmazonReturnLabel:
        """Parse Amazon's label details."""
        return AmazonReturnLabel(
            return_id=return_id,
            label_type=label_data.get("labelType"),
            carrier_name=label_data.get("carrierName"),
            carrier_tracking_id=label_data.get("carrierTrackingId"),
            label_price=float(label_data.get("labelPrice") or 0),
        )
        
    def parse_return(self, return_data: dict) -> AmazonReturn:
        """Parse a return from API response."""
        return AmazonReturn(
            return_request_id=return_data.get("returnRequestId", ""),
            order_id=return_data.get("orderId", ""),
            rma_id=return_data.get("rmaId"),
            marketplace_id=return_data.get("marketplaceId"),
            internal_status=InternalStatus.PENDING_RMA,
            return_request_state=return_data.get("returnRequestState"),
            total_order_value=float(return_data.get("totalOrderValue") or 0),
            currency_code=return_data.get("currencyCode", "EUR"),
            customer_id=return_data.get("customerId"),
            customer_name=return_data.get("customerName"),
            order_date=self.parse_datetime(return_data.get("orderDate")),
            return_request_date=self.parse_datetime(return_data.get("returnRequestDate")),
            approve_date=self.parse_datetime(return_data.get("approveDate")),
            close_date=self.parse_datetime(return_data.get("closeDate")),
            in_policy=return_data.get("inPolicy", True),
            contains_replacement=return_data.get("containsReplacement", False),
            contains_exchange=return_data.get("containsExchange", False),
            sales_channel=return_data.get("salesChannel"),
            refund_status=return_data.get("refundStatus"),
            return_address_id=return_data.get("returnAddressId"),
            shipping_address_id=return_data.get("shippingAddressId"),
            prime_return=return_data.get("primeReturn", False),
            gift_return=return_data.get("giftReturn", False),
            prp_address=return_data.get("prpAddress", False),
            ooc_return=return_data.get("oocReturn", False),
            prime=return_data.get("prime", False),
            ato_z_claim_filed=return_data.get("aToZClaimFiled", False),
            auto_authorized=return_data.get("autoAuthorized", False),
            iba_order=return_data.get("ibaOrder", False),
            replacement_order=return_data.get("replacementOrder", False),
            cosworth_order=return_data.get("cosworthOrder", False),
            has_prior_refund=return_data.get("hasPriorRefund", False),
            raw_data=return_data,
            data_hash=self.compute_hash(return_data),
        )
        
    def ingest_return(self, return_data: dict) -> Tuple[AmazonReturn, bool]:
        """
        Ingest a single return.
        Returns (AmazonReturn, is_new).
        """
        return_request_id = return_data.get("returnRequestId")
        
        # Check if exists
        existing = self.db.query(AmazonReturn).filter(
            AmazonReturn.return_request_id == return_request_id
        ).first()
        
        if existing:
            # Check if data changed
            new_hash = self.compute_hash(return_data)
            if existing.data_hash == new_hash:
                return existing, False
                
            # Update existing record's fields directly
            try:
                existing.order_id = return_data.get("orderId", existing.order_id)
                existing.rma_id = return_data.get("rmaId", existing.rma_id)
                existing.marketplace_id = return_data.get("marketplaceId", existing.marketplace_id)
                existing.return_request_state = return_data.get("returnRequestState", existing.return_request_state)
                existing.total_order_value = float(return_data.get("totalOrderValue") or existing.total_order_value or 0)
                existing.currency_code = return_data.get("currencyCode", existing.currency_code)
                existing.customer_id = return_data.get("customerId", existing.customer_id)
                existing.customer_name = return_data.get("customerName", existing.customer_name)
                existing.order_date = self.parse_datetime(return_data.get("orderDate")) or existing.order_date
                existing.return_request_date = self.parse_datetime(return_data.get("returnRequestDate")) or existing.return_request_date
                existing.approve_date = self.parse_datetime(return_data.get("approveDate")) or existing.approve_date
                existing.close_date = self.parse_datetime(return_data.get("closeDate")) or existing.close_date
                existing.in_policy = return_data.get("inPolicy", existing.in_policy)
                existing.contains_replacement = return_data.get("containsReplacement", existing.contains_replacement)
                existing.contains_exchange = return_data.get("containsExchange", existing.contains_exchange)
                existing.sales_channel = return_data.get("salesChannel", existing.sales_channel)
                existing.refund_status = return_data.get("refundStatus", existing.refund_status)
                existing.return_address_id = return_data.get("returnAddressId", existing.return_address_id)
                existing.shipping_address_id = return_data.get("shippingAddressId", existing.shipping_address_id)
                existing.prime_return = return_data.get("primeReturn", existing.prime_return)
                existing.gift_return = return_data.get("giftReturn", existing.gift_return)
                existing.prp_address = return_data.get("prpAddress", existing.prp_address)
                existing.ooc_return = return_data.get("oocReturn", existing.ooc_return)
                existing.prime = return_data.get("prime", existing.prime)
                existing.ato_z_claim_filed = return_data.get("aToZClaimFiled", existing.ato_z_claim_filed)
                existing.auto_authorized = return_data.get("autoAuthorized", existing.auto_authorized)
                existing.iba_order = return_data.get("ibaOrder", existing.iba_order)
                existing.replacement_order = return_data.get("replacementOrder", existing.replacement_order)
                existing.cosworth_order = return_data.get("cosworthOrder", existing.cosworth_order)
                existing.has_prior_refund = return_data.get("hasPriorRefund", existing.has_prior_refund)
                existing.raw_data = return_data
                existing.data_hash = new_hash
                existing.updated_at = datetime.utcnow()
                
                # Update label if present
                label_details = return_data.get("labelDetails")
                if label_details and existing.amazon_label:
                    existing.amazon_label.label_type = label_details.get("labelType")
                    existing.amazon_label.carrier_name = label_details.get("carrierName")
                    existing.amazon_label.carrier_tracking_id = label_details.get("carrierTrackingId")
                    existing.amazon_label.label_price = float(label_details.get("labelPrice") or 0)
                elif label_details:
                    label = self.parse_label_details(label_details, existing.id)
                    self.db.add(label)
                
                self.db.commit()
                logger.debug(f"Updated return {existing.return_request_id}")
                return existing, False
                
            except Exception as e:
                self.db.rollback()
                logger.error(f"Error updating return {existing.return_request_id}: {e}")
                raise
            
        # Create new return
        amazon_return = self.parse_return(return_data)
        self.db.add(amazon_return)
        self.db.flush()  # Get ID
        
        # Calculate price per item
        items_data = return_data.get("returnRequestItems", [])
        item_count = len(items_data) if items_data else 1
        calculated_price = float(return_data.get("totalOrderValue") or 0) / item_count
        
        # Create items
        for item_data in items_data:
            item = self.parse_item(item_data, amazon_return.id, calculated_price)
            self.db.add(item)
            
        # Create Amazon label if present
        label_details = return_data.get("labelDetails")
        if label_details:
            label = self.parse_label_details(label_details, amazon_return.id)
            self.db.add(label)
            
        return amazon_return, True
        
    async def fetch_and_ingest(self, days_back: int = 90) -> Tuple[int, int]:
        """
        Fetch returns from Amazon and ingest into database.
        
        Args:
            days_back: Number of days back to fetch returns (default: 90)
        
        Returns:
            (total_fetched, new_count)
        """
        logger.info(f"Starting return fetch and ingest ({days_back} days back)...")
        
        # Fetch all returns from Amazon
        returns = await self.client.fetch_all_returns(days_back=days_back)
        
        total_fetched = len(returns)
        new_count = 0
        
        for return_data in returns:
            try:
                _, is_new = self.ingest_return(return_data)
                if is_new:
                    new_count += 1
            except Exception as e:
                logger.error(f"Failed to ingest return: {e}")
                continue
                
        self.db.commit()
        
        logger.info(f"Ingested {new_count} new returns out of {total_fetched} total")
        return total_fetched, new_count
