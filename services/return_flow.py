"""
Return Flow - Main orchestration for return processing.

Handles the complete return processing pipeline:
1. Initialize session (via SessionManager - once per cycle)
2. Fetch returns from Amazon
3. Fetch addresses
4. Wait for RMA data from JTL worker (via queue polling)
5. Detect duplicates
6. Filter eligible returns
7. Generate DHL labels
8. Upload labels to Amazon
9. Update statistics

Key features:
- Session refresh at start of each cycle
- Retry with session reset on 4xx/5xx errors (via RetryWithSessionReset)
- Per-return error tracking with last_error field
- Transaction boundaries per step
- Async queue-based RMA wait
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session

from models.amazon_return import AmazonReturn, InternalStatus, ReturnRequestState
from services.session_manager import SessionManager, SessionExpiredError, session_manager
from services.amazon_client import AmazonClient, HTTPError
from services.fetch_service import FetchService
from services.address_service import AddressService
from services.filter_service import FilterService
from services.label_service import LabelService
from services.upload_service import UploadService
from services.aggregation_service import AggregationService
from services.retry_handler import RetryWithSessionReset
from services.jtl_service import jtl_service
from config import settings

logger = logging.getLogger(__name__)


class ReturnFlow:
    """
    Main orchestration class for return processing.
    
    Usage:
        db = SessionLocal()
        flow = ReturnFlow(db)
        result = await flow.run_cycle(days_back=90)
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    def _parse_datetime(self, value) -> Optional[datetime]:
        """Parse datetime from various formats."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                # Try ISO format first
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                try:
                    # Try common format
                    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return None
        return None

    async def run_cycle(self, days_back: int = 90) -> Dict[str, Any]:
        """
        Run a complete processing cycle.
        
        Args:
            days_back: Number of days to fetch returns for
            
        Returns:
            Summary dict with step results and any errors
        """
        logger.info(f"=== Starting Return Processing Cycle (days_back={days_back}) ===")
        
        summary = {
            "status": "success",
            "started_at": datetime.utcnow().isoformat(),
            "days_back": days_back,
            "steps": {},
            "errors": []
        }
        
        try:
            # ============================================================
            # STEP 0: Initialize session (once per cycle)
            # ============================================================
            logger.info("Step 0: Initializing Amazon session...")
            try:
                session = await session_manager.init_session_for_cycle()
                summary["steps"]["session_init"] = {
                    "status": "success",
                    "refresh_time": session_manager.last_refresh_time.isoformat() if session_manager.last_refresh_time else None
                }
            except SessionExpiredError as e:
                logger.error(f"Failed to initialize session: {e}")
                summary["status"] = "session_error"
                summary["errors"].append(f"Session initialization failed: {str(e)}")
                return summary
                
            # Create services with session and retry handler
            amazon_client = AmazonClient(session_manager, session)
            retry_handler = RetryWithSessionReset(session_manager, amazon_client)
            
            fetch_service = FetchService(self.db, amazon_client, retry_handler)
            address_service = AddressService(self.db, amazon_client, retry_handler)
            filter_service = FilterService(self.db)
            label_service = LabelService(self.db)
            upload_service = UploadService(self.db, amazon_client)
            agg_service = AggregationService(self.db)
            
            # ============================================================
            # STEP 1: Fetch returns from Amazon
            # ============================================================
            logger.info("Step 1: Fetching returns from Amazon...")
            try:
                total_fetched, new_count = await fetch_service.fetch_and_ingest(
                    days_back=days_back
                )
                summary["steps"]["fetch"] = {
                    "status": "success",
                    "total": total_fetched,
                    "new": new_count
                }
                logger.info(f"Fetched {total_fetched} returns ({new_count} new)")
            except HTTPError as e:
                logger.error(f"Fetch failed: {e}")
                summary["steps"]["fetch"] = {"status": "error", "error": str(e)}
                summary["errors"].append(f"Fetch step failed: {str(e)}")
                # Continue with existing data
            except Exception as e:
                logger.error(f"Unexpected fetch error: {e}")
                summary["steps"]["fetch"] = {"status": "error", "error": str(e)}
                summary["errors"].append(f"Fetch step failed: {str(e)}")
                
            # ============================================================
            # STEP 2: Fetch addresses
            # ============================================================
            logger.info("Step 2: Fetching addresses...")
            try:
                addresses_fetched = await address_service.fetch_all_missing_addresses()
                summary["steps"]["addresses"] = {
                    "status": "success",
                    "fetched": addresses_fetched
                }
            except Exception as e:
                logger.error(f"Address fetch error: {e}")
                summary["steps"]["addresses"] = {"status": "error", "error": str(e)}
                summary["errors"].append(f"Address step failed: {str(e)}")
                
            # ============================================================
            # STEP 3: Perform RMA lookup and fetch order details from JTL
            # ============================================================
            logger.info("Step 3: Performing RMA lookup and fetching order details from JTL...")
            
            # Import order detail models
            from models.order_details import (
                OrderGeneralDetails, OrderProductDescription,
                OrderProductSpec, OrderProductAttribute, OrderTrackingInfo
            )
            
            pending_rma_returns = self.db.query(AmazonReturn).filter(
                AmazonReturn.internal_status == InternalStatus.PENDING_RMA
            ).all()
            
            logger.info(f"Found {len(pending_rma_returns)} returns needing RMA lookup")
            
            rma_found_count = 0
            rma_not_found_count = 0
            order_details_stored = 0
            
            if pending_rma_returns:
                try:
                    for amazon_return in pending_rma_returns:
                        order_id = amazon_return.order_id
                        if not order_id:
                            logger.warning(f"Return {amazon_return.return_request_id} has no order_id, skipping")
                            continue
                        
                        # Fetch ALL order data from JTL (RMA + product details)
                        order_data = jtl_service.get_all_order_data(order_id)
                        
                        # Update RMA status
                        rma = order_data.get("internal_rma")
                        if rma:
                            amazon_return.internal_rma = rma
                            amazon_return.internal_status = InternalStatus.RMA_RECEIVED
                            rma_found_count += 1
                            logger.info(f"  ✓ {order_id} -> RMA: {rma}")
                        else:
                            amazon_return.internal_status = InternalStatus.NO_RMA_FOUND
                            rma_not_found_count += 1
                            logger.warning(f"  ✗ {order_id} -> RMA not found")
                        
                        # Store all order details in database
                        return_id = amazon_return.id
                        
                        # Store general details
                        for gd in order_data.get("general_details", []):
                            self.db.add(OrderGeneralDetails(
                                return_id=return_id,
                                jtl_order_id=gd.get("cOrderId"),
                                purchase_date=self._parse_datetime(gd.get("dPurchaseDate")),
                                status_code=gd.get("nStatus"),
                                order_status=gd.get("cOrderStatus"),
                                is_fba=bool(gd.get("nFBA")),
                                is_prime=bool(gd.get("nPrime")),
                                buyer_name=gd.get("cBuyerName"),
                                buyer_email=gd.get("cBuyerEmail"),
                                internal_order_number=gd.get("Internal_Order_Number"),
                                sku=gd.get("SKU"),
                                product_name=gd.get("Product_Name"),
                                asin=gd.get("ASIN"),
                                quantity_purchased=gd.get("nQuantityPurchased", 1)
                            ))
                        
                        # Store product descriptions
                        for pd in order_data.get("product_descriptions", []):
                            self.db.add(OrderProductDescription(
                                return_id=return_id,
                                sku=pd.get("SKU"),
                                internal_id=pd.get("Internal_ID"),
                                display_name=pd.get("Display_Name"),
                                description_html=pd.get("Description_HTML"),
                                short_description=pd.get("Short_Description"),
                                manufacturer=pd.get("Manufacturer"),
                                ean=pd.get("EAN"),
                                mpn=pd.get("MPN")
                            ))
                        
                        # Store product specs
                        for ps in order_data.get("product_specs", []):
                            self.db.add(OrderProductSpec(
                                return_id=return_id,
                                sku=ps.get("SKU"),
                                spec_name=ps.get("Spec_Name"),
                                spec_value=ps.get("Spec_Value")
                            ))
                        
                        # Store product attributes
                        for pa in order_data.get("product_attributes", []):
                            self.db.add(OrderProductAttribute(
                                return_id=return_id,
                                sku=pa.get("SKU"),
                                attribute_name=pa.get("Attribute_Name"),
                                attribute_value=pa.get("Attribute_Value")
                            ))
                        
                        # Store tracking info
                        for ti in order_data.get("tracking_info", []):
                            self.db.add(OrderTrackingInfo(
                                return_id=return_id,
                                tracking_number=ti.get("Tracking_Number"),
                                carrier_id=ti.get("Carrier"),
                                shipped_date=self._parse_datetime(ti.get("Shipped_Date")),
                                delivery_note_date=self._parse_datetime(ti.get("DeliveryNote_Date")),
                                internal_order_number=ti.get("cAuftragsNr")
                            ))
                        
                        order_details_stored += 1
                    
                    self.db.commit()
                    
                    summary["steps"]["rma_lookup"] = {
                        "status": "success",
                        "total": len(pending_rma_returns),
                        "found": rma_found_count,
                        "not_found": rma_not_found_count,
                        "order_details_stored": order_details_stored
                    }
                    logger.info(f"RMA lookup complete: {rma_found_count} found, {rma_not_found_count} not found, {order_details_stored} order details stored")
                    
                except Exception as e:
                    logger.error(f"RMA lookup error: {e}")
                    summary["steps"]["rma_lookup"] = {"status": "error", "error": str(e)}
                    summary["errors"].append(f"RMA lookup failed: {str(e)}")
            else:
                logger.info("No returns need RMA lookup")
                summary["steps"]["rma_lookup"] = {"status": "skipped", "total": 0}
            
            # ============================================================
            # STEP 3.5: Mark completed and already-labelled returns
            # ============================================================
            logger.info("Step 3.5: Checking for completed and already-labelled returns...")
            
            completed_count = 0
            already_labelled_count = 0
            
            # Check returns with RMA
            returns_with_rma = self.db.query(AmazonReturn).filter(
                AmazonReturn.internal_status == InternalStatus.RMA_RECEIVED
            ).all()
            
            for ret in returns_with_rma:
                # FIRST: Check if Amazon state is Completed (takes precedence)
                # Completed returns also have tracking IDs, so check this first
                if ret.return_request_state == ReturnRequestState.COMPLETED:
                    ret.internal_status = InternalStatus.COMPLETED
                    completed_count += 1
                # SECOND: Check if Amazon already provided a tracking label (not completed yet)
                elif ret.amazon_label and ret.amazon_label.carrier_tracking_id:
                    ret.internal_status = InternalStatus.ALREADY_LABEL_SUBMITTED
                    already_labelled_count += 1
            
            self.db.commit()
            
            logger.info(f"✓ Marked {completed_count} as completed, {already_labelled_count} as already-labelled")
            summary["steps"]["status_check"] = {
                "status": "success",
                "completed": completed_count,
                "already_labelled": already_labelled_count
            }
                
            # ============================================================
            # STEP 4: Detect duplicates
            # ============================================================
            logger.info("Step 4: Detecting duplicates...")
            try:
                groups, duplicates = filter_service.detect_duplicates()
                summary["steps"]["duplicates"] = {
                    "status": "success",
                    "groups": groups,
                    "marked": duplicates
                }
            except Exception as e:
                logger.error(f"Duplicate detection error: {e}")
                summary["steps"]["duplicates"] = {"status": "error", "error": str(e)}
                
            # ============================================================
            # STEP 5: Filter eligible returns
            # ============================================================
            logger.info("Step 5: Filtering eligible returns...")
            try:
                eligible_returns = filter_service.get_returns_for_processing()
                summary["steps"]["filter"] = {
                    "status": "success",
                    "eligible_count": len(eligible_returns)
                }
                logger.info(f"Found {len(eligible_returns)} eligible returns")
            except Exception as e:
                logger.error(f"Filter error: {e}")
                summary["steps"]["filter"] = {"status": "error", "error": str(e)}
                eligible_returns = []


            # ============================================================
            # STEP 6: Generate DHL labels
            # ============================================================
            logger.info("Step 6: Generating DHL labels...")
            try:
                generated_count = await label_service.generate_labels_for_eligible()
                summary["steps"]["label_generation"] = {
                    "status": "success",
                    "generated": generated_count
                }
            except Exception as e:
                logger.error(f"Label generation error: {e}")
                summary["steps"]["label_generation"] = {"status": "error", "error": str(e)}
                summary["errors"].append(f"Label generation failed: {str(e)}")
            
            # ============================================================
            # STEP 7: Upload labels to Amazon
            # ============================================================
            logger.info("Step 7: Uploading labels to Amazon...")
            try:
                upload_count = await upload_service.upload_all_pending()
                summary["steps"]["label_upload"] = {
                    "status": "success",
                    "uploaded": upload_count
                }
            except Exception as e:
                logger.error(f"Label upload error: {e}")
                summary["steps"]["label_upload"] = {"status": "error", "error": str(e)}
                summary["errors"].append(f"Label upload failed: {str(e)}")
                
            # ============================================================
            # STEP 8: Update statistics
            # ============================================================
            logger.info("Step 8: Updating statistics...")
            try:
                agg_service.aggregate_all()
                summary["steps"]["aggregation"] = {"status": "success"}
            except Exception as e:
                logger.error(f"Aggregation error: {e}")
                summary["steps"]["aggregation"] = {"status": "error", "error": str(e)}
                

                
            # Final status
            summary["completed_at"] = datetime.utcnow().isoformat()
            if summary["errors"]:
                summary["status"] = "completed_with_errors"
            else:
                summary["status"] = "success"
                
            logger.info(f"=== Cycle Complete ===")
            logger.info(f"Summary: {summary}")
            
            return summary
            
        except SessionExpiredError as e:
            logger.error(f"Session expired during cycle: {e}")
            summary["status"] = "session_error"
            summary["errors"].append(f"Session expired: {str(e)}")
            return summary
            
        except Exception as e:
            logger.error(f"Cycle failed with unexpected error: {e}", exc_info=True)
            summary["status"] = "error"
            summary["errors"].append(f"Unexpected error: {str(e)}")
            return summary
            



async def run_processing_cycle(db: Session, days_back: int = 90) -> Dict[str, Any]:
    """
    Convenience function to run a processing cycle.
    
    Args:
        db: Database session
        days_back: Days to fetch returns for
        
    Returns:
        Cycle summary dict
    """
    flow = ReturnFlow(db)
    return await flow.run_cycle(days_back=days_back)
