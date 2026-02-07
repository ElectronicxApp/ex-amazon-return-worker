"""
Filter Service - Filters and deduplicates returns for processing.

Handles:
- Eligibility filtering (RMA received, state check)
- Duplicate detection (same order_id + ASIN → keep oldest)
- Marking duplicates as closed
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Set
from collections import defaultdict

from sqlalchemy.orm import Session

from models.amazon_return import (
    AmazonReturn, AmazonReturnItem,
    InternalStatus, ReturnRequestState
)
from models.order_details import (
    OrderGeneralDetails,
    OrderProductDescription,
    OrderProductSpec,
    OrderProductAttribute,
    OrderTrackingInfo,
)

logger = logging.getLogger(__name__)


class FilterService:
    """Service for filtering and deduplicating returns."""
    
    def __init__(self, db: Session):
        self.db = db
        
    def get_eligible_states(self) -> List[str]:
        """States that are eligible for processing."""
        return [
            ReturnRequestState.PENDING_LABEL,
            ReturnRequestState.PENDING_APPROVAL,
            ReturnRequestState.PENDING_REFUND,
        ]
        
    def is_eligible_for_processing(self, amazon_return: AmazonReturn) -> Tuple[str, bool]:
        """
        Check if a return is eligible for label generation.
        Returns (reason, is_eligible).
        
        Priority order:
        1. RMA check (foundational requirement)
        2. Address check (needed for label)
        3. Age check (90 days from order)
        4. Policy check (must be in policy)
        5. Amazon state check (must be pending)
        """
        # 1. Must have RMA (first priority - can't process without it)
        if not amazon_return.internal_rma:
            return "NO_RMA", False
        
        # 2. Must have address (needed for DHL label)
        if not amazon_return.address:
            return "NO_ADDRESS", False
        
        # 3. Check if return is too old (90 days from order date)
        if amazon_return.return_request_date and amazon_return.order_date:
            if amazon_return.return_request_date > amazon_return.order_date + timedelta(days=90):
                return "OLD_RETURN", False
        
        # 4. Must be in_policy
        if not amazon_return.in_policy:
            return "NOT_IN_POLICY", False
        
        # 5. Amazon state should be PendingLabel, PendingApproval, or PendingRefund
        if amazon_return.return_request_state not in self.get_eligible_states():
            return f"NOT_ELIGIBLE_STATE: {amazon_return.return_request_state}", False
        
        return "ELIGIBLE", True
        
    def get_returns_for_processing(self) -> List[AmazonReturn]:
        """Get all returns that are ready for label generation."""
        eligible = []
        
        returns = self.db.query(AmazonReturn).filter(
            AmazonReturn.internal_status.in_([
                InternalStatus.RMA_RECEIVED,
            ]),
            AmazonReturn.internal_rma.isnot(None),
        ).all()
        
        for ret in returns:
            reason, is_eligible = self.is_eligible_for_processing(ret)
            if is_eligible:
                ret.internal_status = InternalStatus.ELIGIBLE
                eligible.append(ret)
            else:
                ret.internal_status = InternalStatus.NOT_ELIGIBLE
                ret.last_error = reason
                
        self.db.commit()
        return eligible
        
    def detect_duplicates(self) -> Tuple[int, int]:
        """
        Detect and mark duplicate returns using optimized SQL queries.
        
        A return is a duplicate if ANY of its (order_id, asin) pairs already exist
        in an OLDER return (based on return_request_date).
        
        Logic:
        1. For each return to check (NO_RMA_FOUND, RMA_RECEIVED)
        2. Check if any of its (order_id, asin) pairs exist in an older return
        3. Older returns include ALL statuses EXCEPT DUPLICATE_CLOSED and NOT_ELIGIBLE
        4. If duplicate found, mark as DUPLICATE_CLOSED
        
        Returns (duplicates_found, duplicates_marked).
        """
        from sqlalchemy import and_
        from sqlalchemy.orm import aliased
        
        logger.info("Detecting duplicate returns...")
        
        # Get all returns to check for duplicates
        returns_to_check = self.db.query(AmazonReturn).filter(
            AmazonReturn.internal_status.in_([
                InternalStatus.NO_RMA_FOUND,
                InternalStatus.RMA_RECEIVED,
            ])
        ).all()
        
        logger.info(f"Checking {len(returns_to_check)} returns for duplicates")
        
        duplicates_marked = 0
        duplicate_groups = set()
        
        for ret in returns_to_check:
            # Check each ASIN in this return
            is_duplicate = False
            duplicate_of = None
            duplicate_asin = None
            
            for item in ret.items:
                # Find if there's an older return with same (order_id, asin)
                older_return = (
                    self.db.query(AmazonReturn)
                    .join(AmazonReturnItem)
                    .filter(
                        AmazonReturn.order_id == ret.order_id,
                        AmazonReturnItem.asin == item.asin,
                        AmazonReturn.id != ret.id,
                        AmazonReturn.return_request_date <= ret.return_request_date,
                        AmazonReturn.internal_status.notin_([
                            InternalStatus.DUPLICATE_CLOSED
                        ])
                    )
                    .order_by(AmazonReturn.return_request_date)
                    .first()
                )
                
                if older_return:
                    is_duplicate = True
                    duplicate_of = older_return
                    duplicate_asin = item.asin
                    duplicate_groups.add((ret.order_id, item.asin))
                    break
            
            if is_duplicate and duplicate_of:
                # Delete order details for this duplicate
                self._delete_order_details(ret.id)
                
                # Mark as duplicate
                ret.internal_status = InternalStatus.DUPLICATE_CLOSED
                ret.last_error = f"ASIN {duplicate_asin} already in {duplicate_of.return_request_id}"
                duplicates_marked += 1
                
                logger.info(
                    f"Marked as duplicate: {ret.return_request_id} "
                    f"(order_id={ret.order_id}, asin={duplicate_asin}) -> "
                    f"duplicate of {duplicate_of.return_request_id}"
                )
        
        self.db.commit()
        
        logger.info(f"Duplicate detection: {len(duplicate_groups)} groups, {duplicates_marked} marked")
        return len(duplicate_groups), duplicates_marked
    

    def _delete_order_details(self, return_id: int):
        """
        Delete all order details for a return.
        Called when detecting duplicates to avoid storing redundant data.
        """
        # Delete from all 5 order details tables
        self.db.query(OrderGeneralDetails).filter(
            OrderGeneralDetails.return_id == return_id
        ).delete(synchronize_session=False)
        
        self.db.query(OrderProductDescription).filter(
            OrderProductDescription.return_id == return_id
        ).delete(synchronize_session=False)
        
        self.db.query(OrderProductSpec).filter(
            OrderProductSpec.return_id == return_id
        ).delete(synchronize_session=False)
        
        self.db.query(OrderProductAttribute).filter(
            OrderProductAttribute.return_id == return_id
        ).delete(synchronize_session=False)
        
        self.db.query(OrderTrackingInfo).filter(
            OrderTrackingInfo.return_id == return_id
        ).delete(synchronize_session=False)
        
        logger.debug(f"Deleted order details for return_id={return_id}")
            
    def get_processing_summary(self) -> Dict[str, int]:
        """Get summary of returns by status."""
        summary = {}
        
        for status in [
            InternalStatus.PENDING_RMA,
            InternalStatus.NO_RMA_FOUND,
            InternalStatus.RMA_RECEIVED,
            InternalStatus.DUPLICATE_CLOSED,
            InternalStatus.NOT_ELIGIBLE,
            InternalStatus.ALREADY_LABEL_SUBMITTED,
            InternalStatus.ELIGIBLE,
            InternalStatus.LABEL_GENERATED,
            InternalStatus.LABEL_UPLOADED,
            InternalStatus.LABEL_SUBMITTED,
            InternalStatus.PROCESSING_ERROR,
            InternalStatus.COMPLETED,
        ]:
            count = self.db.query(AmazonReturn).filter(
                AmazonReturn.internal_status == status
            ).count()
            summary[status] = count
            
        return summary
    
    def get_pending_rma_returns(self) -> List[AmazonReturn]:
        """
        Get returns that need RMA lookup.
        
        Returns:
            List of returns with PENDING_RMA status
        """
        returns = (
            self.db.query(AmazonReturn)
            .filter(AmazonReturn.internal_status == InternalStatus.PENDING_RMA)
            .all()
        )
        
        logger.info(f"Found {len(returns)} returns pending RMA")
        return returns
        
    def mark_eligible(self, amazon_return: AmazonReturn):
        """Mark a return as eligible for processing."""
        amazon_return.internal_status = InternalStatus.ELIGIBLE
        amazon_return.last_error = None
        self.db.flush()
        
    def mark_no_rma(self, amazon_return: AmazonReturn, reason: str = "No RMA found in JTL"):
        """Mark a return as having no RMA."""
        amazon_return.internal_status = InternalStatus.NO_RMA_FOUND
        amazon_return.last_error = reason
        self.db.flush()
        
    def mark_rma_received(self, amazon_return: AmazonReturn, internal_rma: str):
        """Mark a return as having received RMA."""
        amazon_return.internal_status = InternalStatus.RMA_RECEIVED
        amazon_return.internal_rma = internal_rma
        amazon_return.last_error = None
        self.db.flush()
        
    def get_statistics_summary(self) -> dict:
        """Get summary statistics for filtering."""
        return self.get_processing_summary()
