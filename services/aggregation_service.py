"""
Aggregation Service - Computes statistics for dashboard.

Handles:
- Daily statistics aggregation for trend charts
- Reason breakdown aggregation
- Marketplace breakdown aggregation
- Top products aggregation
- Global metrics aggregation

Features:
- Incremental or full aggregation
- 4-value metrics: today, this_week, all_time (in_range calculated live)
- Labels created from AmazonReturnGeneratedLabel (excluding ERROR)
- Auto-processed tracking
- Error tracking (no_rma_found, processing_errors, duplicates_filtered)
"""

import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional

from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from models.amazon_return import (
    AmazonReturn,
    AmazonReturnItem,
    AmazonReturnGeneratedLabel,
    InternalStatus,
    LabelState,
)
from models.statistics import (
    StatisticsDaily,
    StatisticsReason,
    StatisticsMarketplace,
    StatisticsTopProducts,
    StatisticsGlobal,
    MARKETPLACE_COUNTRY_MAP,
    REASON_DISPLAY_MAP,
)

logger = logging.getLogger(__name__)


class AggregationService:
    """Service for computing and storing aggregated statistics."""
    
    def __init__(self, db: Session):
        self.db = db
        
    def aggregate_daily(self, target_date: date = None) -> StatisticsDaily:
        """
        Aggregate statistics for a specific date.
        
        Args:
            target_date: Date to aggregate (defaults to today)
            
        Returns:
            StatisticsDaily record
        """
        if target_date is None:
            target_date = date.today()
            
        logger.info(f"Aggregating daily statistics for {target_date}")
        
        # Get or create daily record
        daily = self.db.query(StatisticsDaily).filter(
            StatisticsDaily.stat_date == target_date
        ).first()
        
        if not daily:
            daily = StatisticsDaily(stat_date=target_date)
            self.db.add(daily)
            
        # Date range for target date
        start_dt = datetime.combine(target_date, datetime.min.time())
        end_dt = datetime.combine(target_date, datetime.max.time())
        
        # Total returns with return_request_date on this day
        daily.total_returns = self.db.query(AmazonReturn).filter(
            AmazonReturn.return_request_date >= start_dt,
            AmazonReturn.return_request_date <= end_dt,
        ).count()
        
        # Newly fetched returns (created_at) on this day - these are newly fetched returns
        daily.newly_fetched = self.db.query(AmazonReturn).filter(
            AmazonReturn.created_at >= start_dt,
            AmazonReturn.created_at <= end_dt,
        ).count()
        
        # Statuses to exclude from actual refund calculations (duplicates and ineligible returns don't count)
        excluded_statuses = [InternalStatus.DUPLICATE_CLOSED, InternalStatus.NOT_ELIGIBLE]
        
        # Requested refund amount (ALL returns, no filtering)
        requested_refund = self.db.query(
            func.coalesce(func.sum(AmazonReturn.total_order_value), 0)
        ).filter(
            AmazonReturn.return_request_date >= start_dt,
            AmazonReturn.return_request_date <= end_dt,
        ).scalar()
        daily.requested_refund_amount = requested_refund or 0
        
        # Actual refund amount (filtered - excludes duplicates/not eligible)
        actual_refund = self.db.query(
            func.coalesce(func.sum(AmazonReturn.total_order_value), 0)
        ).filter(
            AmazonReturn.return_request_date >= start_dt,
            AmazonReturn.return_request_date <= end_dt,
            ~AmazonReturn.internal_status.in_(excluded_statuses),
        ).scalar()
        daily.actual_refund_amount = actual_refund or 0
        
        # Status breakdown (essential statuses only)
        status_counts = self.db.query(
            AmazonReturn.internal_status,
            func.count(AmazonReturn.id)
        ).filter(
            AmazonReturn.return_request_date >= start_dt,
            AmazonReturn.return_request_date <= end_dt,
        ).group_by(AmazonReturn.internal_status).all()
        
        # Reset essential status counts
        daily.status_pending_rma = 0
        daily.status_no_rma_found = 0
        daily.status_duplicate_closed = 0
        daily.status_not_eligible = 0
        daily.status_processing_error = 0
        daily.status_completed = 0
        
        for status, count in status_counts:
            if status == InternalStatus.PENDING_RMA:
                daily.status_pending_rma = count
            elif status == InternalStatus.NO_RMA_FOUND:
                daily.status_no_rma_found = count
            elif status == InternalStatus.DUPLICATE_CLOSED:
                daily.status_duplicate_closed = count
            elif status == InternalStatus.NOT_ELIGIBLE:
                daily.status_not_eligible = count
            elif status == InternalStatus.PROCESSING_ERROR:
                daily.status_processing_error = count
        
        # Count completed returns by close_date (not return_request_date)
        daily.status_completed = self.db.query(AmazonReturn).filter(
            AmazonReturn.close_date >= start_dt,
            AmazonReturn.close_date <= end_dt,
            AmazonReturn.internal_status == InternalStatus.COMPLETED
        ).count()
                
        # Labels created on this date (from AmazonReturnGeneratedLabel, excluding ERROR)
        daily.labels_created = self.db.query(AmazonReturnGeneratedLabel).filter(
            AmazonReturnGeneratedLabel.created_at >= start_dt,
            AmazonReturnGeneratedLabel.created_at <= end_dt,
            AmazonReturnGeneratedLabel.state != LabelState.ERROR,
        ).count()
        
        # Labels submitted (label_submitted_at on this day)
        daily.labels_submitted = self.db.query(AmazonReturn).filter(
            AmazonReturn.label_submitted_at >= start_dt,
            AmazonReturn.label_submitted_at <= end_dt,
        ).count()
        
        # Flag counts
        daily.prime_returns = self.db.query(AmazonReturn).filter(
            AmazonReturn.prime_return == True,
            AmazonReturn.return_request_date >= start_dt,
            AmazonReturn.return_request_date <= end_dt,
        ).count()
        
        daily.gift_returns = self.db.query(AmazonReturn).filter(
            AmazonReturn.gift_return == True,
            AmazonReturn.return_request_date >= start_dt,
            AmazonReturn.return_request_date <= end_dt,
        ).count()
        
        daily.ato_z_claims = self.db.query(AmazonReturn).filter(
            AmazonReturn.ato_z_claim_filed == True,
            AmazonReturn.return_request_date >= start_dt,
            AmazonReturn.return_request_date <= end_dt,
        ).count()
        
        daily.updated_at = datetime.utcnow()
        daily.computed_at = datetime.utcnow()
        
        self.db.flush()
        logger.info(f"Daily stats for {target_date}: {daily.total_returns} returns, {daily.newly_fetched} newly fetched, {daily.labels_created} labels")
        return daily
        
    def aggregate_daily_range(self, days: int = 90, force: bool = False) -> int:
        """
        Aggregate daily statistics for a range of days.
        
        Args:
            days: Number of days to aggregate
            force: If True, recompute even if exists
            
        Returns:
            Number of days aggregated
        """
        logger.info(f"Aggregating daily statistics for last {days} days (force={force})")
        
        count = 0
        today = date.today()
        
        for i in range(days):
            target_date = today - timedelta(days=i)
            
            # Skip if exists and not force
            if not force:
                existing = self.db.query(StatisticsDaily).filter(
                    StatisticsDaily.stat_date == target_date
                ).first()
                if existing:
                    continue
                    
            self.aggregate_daily(target_date)
            count += 1
            
        self.db.flush()
        logger.info(f"Aggregated {count} daily records")
        return count
        
    def aggregate_reasons(self) -> int:
        """
        Aggregate statistics by return reason.
        
        Returns:
            Number of reason records updated
        """
        logger.info("Aggregating reason statistics")
        
        # Clear existing
        self.db.query(StatisticsReason).delete()
        
        # Statuses to exclude
        excluded_statuses = [InternalStatus.DUPLICATE_CLOSED, InternalStatus.NOT_ELIGIBLE]
        
        # Aggregate by reason (excluding duplicates and not eligible)
        reason_stats = self.db.query(
            AmazonReturnItem.return_reason_code,
            func.count(AmazonReturnItem.id),
            func.coalesce(func.sum(func.coalesce(AmazonReturnItem.unit_price, AmazonReturnItem.calculated_price, 0)), 0),
        ).join(
            AmazonReturn, AmazonReturnItem.return_id == AmazonReturn.id
        ).filter(
            AmazonReturnItem.return_reason_code.isnot(None),
            ~AmazonReturn.internal_status.in_(excluded_statuses),
        ).group_by(AmazonReturnItem.return_reason_code).all()
        
        count = 0
        for reason_code, total_count, total_value in reason_stats:
            if not reason_code:
                continue
                
            reason = StatisticsReason(
                reason_code=reason_code,
                reason_display=REASON_DISPLAY_MAP.get(reason_code, reason_code),
                total_count=total_count,
                total_value=total_value or 0,
                avg_value=(total_value or 0) / total_count if total_count > 0 else 0,
            )
            self.db.add(reason)
            count += 1
            
        self.db.flush()
        logger.info(f"Aggregated {count} reason records")
        return count
        
    def aggregate_marketplaces(self) -> int:
        """
        Aggregate statistics by marketplace.
        
        Returns:
            Number of marketplace records updated
        """
        logger.info("Aggregating marketplace statistics")
        
        # Clear existing
        self.db.query(StatisticsMarketplace).delete()
        
        # Statuses to exclude
        excluded_statuses = [InternalStatus.DUPLICATE_CLOSED, InternalStatus.NOT_ELIGIBLE]
        
        # Aggregate by marketplace (excluding duplicates and not eligible)
        mp_stats = self.db.query(
            AmazonReturn.marketplace_id,
            func.count(AmazonReturn.id),
            func.coalesce(func.sum(AmazonReturn.total_order_value), 0),
        ).filter(
            AmazonReturn.marketplace_id.isnot(None),
            ~AmazonReturn.internal_status.in_(excluded_statuses),
        ).group_by(AmazonReturn.marketplace_id).all()
        
        count = 0
        for mp_id, total_count, total_value in mp_stats:
            if not mp_id:
                continue
            
            # Get country info from mapping
            country_info = MARKETPLACE_COUNTRY_MAP.get(mp_id, ("Unknown", "XX"))
                
            marketplace = StatisticsMarketplace(
                marketplace_id=mp_id,
                marketplace_name=country_info[0],
                country_code=country_info[1],
                total_count=total_count,
                total_value=total_value or 0,
                avg_value=(total_value or 0) / total_count if total_count > 0 else 0,
            )
            self.db.add(marketplace)
            count += 1
            
        self.db.flush()
        logger.info(f"Aggregated {count} marketplace records")
        return count
        
    def aggregate_top_products(self, limit: int = 50) -> int:
        """
        Aggregate top returned products.
        
        Args:
            limit: Maximum number of products to track
            
        Returns:
            Number of product records
        """
        logger.info("Aggregating top products")
        
        # Clear existing
        self.db.query(StatisticsTopProducts).delete()
        self.db.flush()
        
        # Statuses to exclude
        excluded_statuses = [InternalStatus.DUPLICATE_CLOSED, InternalStatus.NOT_ELIGIBLE]
        
        # Aggregate by ASIN only - use func.max to get one value for other fields
        # Exclude duplicates and not eligible returns
        product_stats = self.db.query(
            AmazonReturnItem.asin,
            func.max(AmazonReturnItem.product_title).label('product_title'),
            func.max(AmazonReturnItem.product_image_link).label('product_image_link'),
            func.max(AmazonReturnItem.merchant_sku).label('merchant_sku'),
            func.count(AmazonReturnItem.id).label('return_count'),
            func.coalesce(func.sum(func.coalesce(AmazonReturnItem.unit_price, AmazonReturnItem.calculated_price, 0)), 0).label('total_value'),
        ).join(
            AmazonReturn, AmazonReturnItem.return_id == AmazonReturn.id
        ).filter(
            AmazonReturnItem.asin.isnot(None),
            ~AmazonReturn.internal_status.in_(excluded_statuses),
        ).group_by(
            AmazonReturnItem.asin  # Only group by ASIN to ensure uniqueness
        ).order_by(desc('return_count')).limit(limit).all()
        
        count = 0
        seen_asins = set()  # Track seen ASINs to avoid duplicates
        
        for stats in product_stats:
            # Skip if we've already processed this ASIN
            if stats.asin in seen_asins:
                continue
            seen_asins.add(stats.asin)
            
            # Get top reason for this ASIN
            top_reason_result = self.db.query(
                AmazonReturnItem.return_reason_code,
                func.count(AmazonReturnItem.id).label('cnt')
            ).filter(
                AmazonReturnItem.asin == stats.asin,
                AmazonReturnItem.return_reason_code.isnot(None)
            ).group_by(
                AmazonReturnItem.return_reason_code
            ).order_by(desc('cnt')).first()
            
            top_reason = top_reason_result[0] if top_reason_result else None
            
            count += 1  # Increment first for rank
            
            product = StatisticsTopProducts(
                asin=stats.asin,
                product_title=stats.product_title,
                product_image_url=stats.product_image_link,
                merchant_sku=stats.merchant_sku,
                return_count=stats.return_count,
                total_value=stats.total_value or 0,
                top_reason_code=top_reason,
                rank=count,  # Use count as rank
            )
            self.db.add(product)
            
        self.db.flush()
        logger.info(f"Aggregated {count} top product records")
        return count
        
    def aggregate_global(self) -> StatisticsGlobal:
        """
        Aggregate global statistics.
        
        Updates the single-row global metrics table with:
        - Returns: today, this_week, all_time
        - Refunds: today, this_week, all_time
        - Labels: today, this_week, all_time (from AmazonReturnGeneratedLabel, excluding ERROR)
        - Auto-processed: today, this_week, all_time
        - Error tracking: no_rma_found, processing_errors, duplicates_filtered
        - Special flags: prime, ato_z, gift returns
        - Time saved
        
        Returns:
            StatisticsGlobal record
        """
        logger.info("Aggregating global statistics")
        
        # Get or create global record (single row)
        global_stats = self.db.query(StatisticsGlobal).first()
        
        if not global_stats:
            global_stats = StatisticsGlobal()
            self.db.add(global_stats)
            
        # Time boundaries
        now = datetime.utcnow()
        today_start = datetime.combine(date.today(), datetime.min.time())
        week_start = now - timedelta(days=7)
        
        # =====================================================
        # RETURNS (today, this_week, all_time)
        # =====================================================
        global_stats.returns_today = self.db.query(AmazonReturn).filter(
            AmazonReturn.return_request_date >= today_start
        ).count()
        
        global_stats.returns_this_week = self.db.query(AmazonReturn).filter(
            AmazonReturn.return_request_date >= week_start
        ).count()
        
        global_stats.returns_all_time = self.db.query(AmazonReturn).count()
        
        # =====================================================
        # ACTUAL REFUNDS (filtered - excludes duplicates/not eligible)
        # =====================================================
        excluded_statuses = [InternalStatus.DUPLICATE_CLOSED, InternalStatus.NOT_ELIGIBLE]
        
        global_stats.actual_refund_today = self.db.query(
            func.coalesce(func.sum(AmazonReturn.total_order_value), 0)
        ).filter(
            AmazonReturn.return_request_date >= today_start,
            ~AmazonReturn.internal_status.in_(excluded_statuses),
        ).scalar() or 0
        
        global_stats.actual_refund_this_week = self.db.query(
            func.coalesce(func.sum(AmazonReturn.total_order_value), 0)
        ).filter(
            AmazonReturn.return_request_date >= week_start,
            ~AmazonReturn.internal_status.in_(excluded_statuses),
        ).scalar() or 0
        
        global_stats.actual_refund_all_time = self.db.query(
            func.coalesce(func.sum(AmazonReturn.total_order_value), 0)
        ).filter(
            ~AmazonReturn.internal_status.in_(excluded_statuses),
        ).scalar() or 0
        
        # =====================================================
        # REQUESTED REFUNDS (unfiltered - all returns)
        # =====================================================
        global_stats.requested_refund_today = self.db.query(
            func.coalesce(func.sum(AmazonReturn.total_order_value), 0)
        ).filter(
            AmazonReturn.return_request_date >= today_start,
        ).scalar() or 0
        
        global_stats.requested_refund_this_week = self.db.query(
            func.coalesce(func.sum(AmazonReturn.total_order_value), 0)
        ).filter(
            AmazonReturn.return_request_date >= week_start,
        ).scalar() or 0
        
        global_stats.requested_refund_all_time = self.db.query(
            func.coalesce(func.sum(AmazonReturn.total_order_value), 0)
        ).scalar() or 0
        
        # =====================================================
        # LABELS CREATED (today, this_week, all_time) - excluding ERROR
        # =====================================================
        label_base_filter = AmazonReturnGeneratedLabel.state != LabelState.ERROR
        
        global_stats.labels_created_today = self.db.query(AmazonReturnGeneratedLabel).filter(
            AmazonReturnGeneratedLabel.created_at >= today_start,
            label_base_filter
        ).count()
        
        global_stats.labels_created_this_week = self.db.query(AmazonReturnGeneratedLabel).filter(
            AmazonReturnGeneratedLabel.created_at >= week_start,
            label_base_filter
        ).count()
        
        global_stats.labels_created_all_time = self.db.query(AmazonReturnGeneratedLabel).filter(
            label_base_filter
        ).count()
        
        # =====================================================
        # NEWLY FETCHED RETURNS (today, this_week, all_time) - based on created_at
        # =====================================================
        global_stats.newly_fetched_today = self.db.query(AmazonReturn).filter(
            AmazonReturn.created_at >= today_start
        ).count()
        
        global_stats.newly_fetched_this_week = self.db.query(AmazonReturn).filter(
            AmazonReturn.created_at >= week_start
        ).count()
        
        global_stats.newly_fetched_all_time = self.db.query(AmazonReturn).count()
        
        # =====================================================
        # INTERNAL STATUS BREAKDOWN (all-time counts)
        # =====================================================
        global_stats.status_pending_rma = self.db.query(AmazonReturn).filter(
            AmazonReturn.internal_status == InternalStatus.PENDING_RMA
        ).count()
        
        global_stats.status_no_rma_found = self.db.query(AmazonReturn).filter(
            AmazonReturn.internal_status == InternalStatus.NO_RMA_FOUND
        ).count()
        
        global_stats.status_duplicate_closed = self.db.query(AmazonReturn).filter(
            AmazonReturn.internal_status == InternalStatus.DUPLICATE_CLOSED
        ).count()
        
        global_stats.status_not_eligible = self.db.query(AmazonReturn).filter(
            AmazonReturn.internal_status == InternalStatus.NOT_ELIGIBLE
        ).count()
        
        global_stats.status_processing_error = self.db.query(AmazonReturn).filter(
            AmazonReturn.internal_status == InternalStatus.PROCESSING_ERROR
        ).count()
        
        global_stats.status_completed = self.db.query(AmazonReturn).filter(
            AmazonReturn.internal_status == InternalStatus.COMPLETED
        ).count()
        
        # =====================================================
        # SPECIAL FLAGS (all-time)
        # =====================================================
        global_stats.prime_returns_all_time = self.db.query(AmazonReturn).filter(
            AmazonReturn.prime_return == True
        ).count()
        
        global_stats.ato_z_claims_all_time = self.db.query(AmazonReturn).filter(
            AmazonReturn.ato_z_claim_filed == True
        ).count()
        
        global_stats.gift_returns_all_time = self.db.query(AmazonReturn).filter(
            AmazonReturn.gift_return == True
        ).count()
        
        # =====================================================
        # MEDIAN VALUES (for trend chart reference lines)
        # =====================================================
        # Calculate median returns from StatisticsDaily
        daily_stats = self.db.query(StatisticsDaily).all()
        
        daily_returns = [d.total_returns for d in daily_stats if d.total_returns and d.total_returns > 0]
        if daily_returns:
            sorted_returns = sorted(daily_returns)
            mid = len(sorted_returns) // 2
            if len(sorted_returns) % 2 == 0:
                global_stats.median_returns = (sorted_returns[mid-1] + sorted_returns[mid]) / 2
            else:
                global_stats.median_returns = sorted_returns[mid]
        else:
            global_stats.median_returns = 0
        
        # Calculate median refunds from StatisticsDaily
        daily_refunds = [float(d.requested_refund_amount) for d in daily_stats if d.requested_refund_amount and float(d.requested_refund_amount) > 0]
        if daily_refunds:
            sorted_refunds = sorted(daily_refunds)
            mid = len(sorted_refunds) // 2
            if len(sorted_refunds) % 2 == 0:
                global_stats.median_requested_refunds = (sorted_refunds[mid-1] + sorted_refunds[mid]) / 2
            else:
                global_stats.median_requested_refunds = sorted_refunds[mid]
        else:
            global_stats.median_requested_refunds = 0
        
        logger.info(f"Median values: returns={global_stats.median_returns}, refunds={global_stats.median_requested_refunds}")
        
        
        global_stats.updated_at = datetime.utcnow()
        global_stats.computed_at = datetime.utcnow()
        
        self.db.flush()
        logger.info(f"Global stats: {global_stats.returns_all_time} all-time returns, "
                   f"{global_stats.newly_fetched_all_time} newly fetched")
        return global_stats
        
    def aggregate_all(self, days: int = 90, force: bool = False):
        """
        Run all aggregation functions.
        
        Args:
            days: Number of days for daily aggregation
            force: If True, recompute even if exists
        """
        logger.info(f"=== Starting full aggregation (days={days}, force={force}) ===")
        
        # Aggregate daily for range
        daily_count = self.aggregate_daily_range(days=days, force=force)
        
        # Aggregate breakdowns
        reasons_count = self.aggregate_reasons()
        marketplace_count = self.aggregate_marketplaces()
        products_count = self.aggregate_top_products()
        
        # Aggregate global
        self.aggregate_global()
        
        self.db.commit()
        
        logger.info(f"=== Aggregation complete: {daily_count} daily, "
                   f"{reasons_count} reasons, {marketplace_count} marketplaces, "
                   f"{products_count} products ===")
        
        return {
            "daily_records": daily_count,
            "reason_records": reasons_count,
            "marketplace_records": marketplace_count,
            "product_records": products_count,
        }


def run_aggregation_pipeline(db: Session, days: int = 90, force: bool = False) -> Dict[str, Any]:
    """
    Run the complete aggregation pipeline.
    
    This is the main entry point for scheduled aggregation.
    
    Args:
        db: Database session
        days: Number of days to aggregate
        force: If True, recompute even if exists
        
    Returns:
        Summary dict with counts
    """
    service = AggregationService(db)
    return service.aggregate_all(days=days, force=force)
