"""
Upload Service - Uploads labels to Amazon Seller Central.

Handles the complete upload flow:
1. Get CSRF token
2. Download label PDF from S3
3. Convert PDF to PNG (Amazon requires PNG)
4. Get S3 bucket args from Amazon
5. Upload PNG to Amazon's S3
6. Get Alexandria args
7. Upload to Alexandria (returns document_version_id)
8. Complete return with label details
9. Refetch return to get latest state
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from models.amazon_return import (
    AmazonReturn,
    AmazonReturnGeneratedLabel,
    InternalStatus,
    LabelState,
)
from services.amazon_client import AmazonClient, HTTPError
from services.label_service import S3Service
from utils import convert_pdf_to_png_bytes, parse_csrf, generate_s3_key

logger = logging.getLogger(__name__)

# Constant merchant party ID - doesn't need to be stored in database
MERCHANT_PARTY_ID = "A3TZZ7DOC6G9UH"


class UploadService:
    """
    Service for uploading DHL labels to Amazon Seller Central.
    
    Complete flow:
    1. Get CSRF token
    2. Download label PDF from our S3
    3. Convert PDF to PNG
    4. Get S3 bucket args from Amazon
    5. Upload PNG to Amazon's S3
    6. Get Alexandria args
    7. Upload to Alexandria → get document_version_id
    8. Complete return with tracking + document_version_id
    9. Refetch return to get latest state
    """
    
    def __init__(self, db: Session, amazon_client: AmazonClient):
        self.db = db
        self.amazon_client = amazon_client
        self.s3_service = S3Service()
        self._csrf_token: Optional[str] = None
        
    async def _get_csrf_token(self) -> str:
        """Get CSRF token from Amazon."""
        if self._csrf_token:
            return self._csrf_token
            
        # Fetch returns page to get CSRF
        session = self.amazon_client.session
        if not session:
            raise HTTPError("No session available", 401)
            
        url = "https://sellercentral.amazon.de/gp/returns/list/v2"
        response = session.get(url)
        
        if response.status_code != 200:
            raise HTTPError(f"Failed to get CSRF: {response.status_code}", response.status_code)
            
        csrf = parse_csrf(response.text)
        if not csrf:
            raise HTTPError("CSRF token not found in response", 500)
            
        self._csrf_token = csrf
        logger.info(f"[Upload] CSRF token obtained: {csrf[:10]}...")
        return csrf
        
    async def upload_label(self, amazon_return: AmazonReturn) -> bool:
        """
        Upload a generated label to Amazon.
        
        Complete flow:
        1. Validate label exists
        2. Download PDF from our S3
        3. Convert PDF to PNG
        4. Get CSRF token
        5. Get S3 bucket args from Amazon
        6. Upload PNG to Amazon's S3
        7. Get return details (for customer_id, seller_id)
        8. Get Alexandria args
        9. Upload to Alexandria → get document_version_id
        10. Complete return with tracking + document_version_id
        11. Refetch return and update status
        
        Args:
            amazon_return: Return with generated label
            
        Returns:
            True if upload successful
        """
        if not amazon_return.generated_label:
            logger.error(f"[Upload] Return {amazon_return.return_request_id} has no generated label")
            return False
            
        label = amazon_return.generated_label
        
        if not label.s3_key:
            logger.error(f"[Upload] Label has no S3 key: {amazon_return.return_request_id}")
            return False
        
        step = "init"
        try:
            # ============================================================
            # STEP 1: Download label PDF from our S3
            # ============================================================
            step = "download_pdf"
            logger.info(f"[Upload] Step 1: Downloading label from S3: {label.s3_key}")
            
            label_pdf = self.s3_service.get_label(label.s3_key)
            if not label_pdf:
                raise Exception(f"Failed to download label from S3: {label.s3_key}")
            
            logger.info(f"[Upload] ✅ Downloaded {len(label_pdf)} bytes")
            
            # ============================================================
            # STEP 2: Convert PDF to PNG
            # ============================================================
            step = "convert_png"
            logger.info("[Upload] Step 2: Converting PDF to PNG...")
            
            label_png = convert_pdf_to_png_bytes(label_pdf)
            logger.info(f"[Upload] ✅ Converted to PNG: {len(label_png)} bytes")
            
            # ============================================================
            # STEP 3: Get CSRF token
            # ============================================================
            step = "csrf"
            logger.info("[Upload] Step 3: Getting CSRF token...")
            csrf = await self._get_csrf_token()
            
            # ============================================================
            # STEP 4: Get S3 bucket args from Amazon
            # ============================================================
            step = "s3_args"
            logger.info("[Upload] Step 4: Getting Amazon S3 bucket args...")
            
            s3_args = await self.amazon_client.get_s3_bucket_args(
                return_request_id=amazon_return.return_request_id,
                marketplace_id=amazon_return.marketplace_id,
            )
            
            if not s3_args:
                raise Exception("Failed to get S3 arguments from Amazon")
            
            logger.info(f"[Upload] ✅ S3 bucket: {s3_args.get('bucketName')}")
            
            # ============================================================
            # STEP 5: Upload PNG to Amazon's S3
            # ============================================================
            step = "s3_upload"
            logger.info("[Upload] Step 5: Uploading to Amazon S3...")
            
            # Generate unique S3 key for Amazon upload
            amazon_s3_key = generate_s3_key(amazon_return.return_request_id)
            
            s3_upload_success = await self.amazon_client.upload_to_s3(
                s3_args=s3_args,
                label_binary=label_png,
                key=amazon_s3_key,
                csrf=csrf,
            )
            
            if not s3_upload_success:
                raise Exception("S3 upload to Amazon failed")
            
            logger.info("[Upload] ✅ S3 upload successful")
            
            # Update label state to UPLOADED
            label.state = LabelState.UPLOADED
            amazon_return.internal_status = InternalStatus.LABEL_UPLOADED
            self.db.flush()
            
            # ============================================================
            # STEP 6: Get return details for customer_id, seller_id
            # ============================================================
            step = "return_details"
            logger.info("[Upload] Step 6: Getting return details...")
            
            particulars = await self.amazon_client.get_return_details(
                return_request_id=amazon_return.return_request_id,
                marketplace_id=amazon_return.marketplace_id,
            )
            
            if not particulars:
                raise Exception("Failed to get return details")
            
            ret_req = particulars.get('returnRequest', {})
            customer_id = ret_req.get('customerId') or amazon_return.customer_id
            seller_id = MERCHANT_PARTY_ID  # Constant - no need to fetch from API
            address_id = ret_req.get('returnAddressId') or amazon_return.return_address_id
            
            if not customer_id:
                raise Exception("Missing customerId for Alexandria upload")
            
            logger.info(f"[Upload] ✅ Customer: {customer_id}, Seller: {seller_id}")
            
            # ============================================================
            # STEP 7: Get Alexandria args
            # ============================================================
            step = "alexandria_args"
            logger.info("[Upload] Step 7: Getting Alexandria args...")
            
            alex_args = await self.amazon_client.get_alexandria_args(
                customer_id=customer_id,
                marketplace_id=amazon_return.marketplace_id or "",
                seller_id=seller_id,
                return_request_id=amazon_return.return_request_id,
                csrf=csrf,
            )
            
            if not alex_args:
                raise Exception("Failed to get Alexandria args")
            
            logger.info("[Upload] ✅ Alexandria args obtained")
            
            # ============================================================
            # STEP 8: Upload to Alexandria
            # ============================================================
            step = "alexandria_upload"
            logger.info("[Upload] Step 8: Uploading to Alexandria...")
            
            alex_result = await self.amazon_client.alexandria_upload(
                alex_args=alex_args,
                label_png=label_png,
                customer_id=customer_id,
                seller_id=seller_id,
                marketplace_id=amazon_return.marketplace_id or "",
                return_request_id=amazon_return.return_request_id,
                csrf=csrf,
            )
            
            if not alex_result or not alex_result.get('success'):
                raise Exception(f"Alexandria upload failed: {alex_result}")
            
            doc_version_id = alex_result.get('doc_version_id')
            if not doc_version_id:
                raise Exception("Alexandria upload succeeded but no document_version_id returned")
            
            logger.info(f"[Upload] ✅ Alexandria upload successful. DocId: {doc_version_id}")
            
            # ============================================================
            # STEP 9: Complete return
            # ============================================================
            step = "complete_return"
            logger.info("[Upload] Step 9: Completing return...")
            
            complete_success = await self.amazon_client.complete_return(
                return_request_id=amazon_return.return_request_id,
                marketplace_id=amazon_return.marketplace_id or "",
                seller_id=seller_id,
                document_id=doc_version_id,
                address_id=address_id,
                tracking_id=label.tracking_number,
                csrf=csrf,
            )
            
            if not complete_success:
                raise Exception("Complete return request failed")
            
            logger.info("[Upload] ✅ Return completed on Amazon!")
            
            # Update statuses
            label.state = LabelState.SUBMITTED
            label.submitted_at = datetime.utcnow()
            amazon_return.internal_status = InternalStatus.LABEL_SUBMITTED
            amazon_return.label_submitted_at = datetime.utcnow()
            amazon_return.last_error = None
            
            # ============================================================
            # STEP 10: Refetch return to get latest state
            # ============================================================
            step = "refetch"
            logger.info("[Upload] Step 10: Refetching return for latest state...")
            
            refreshed_data = await self.amazon_client.refetch_return(
                return_request_id=amazon_return.return_request_id,
                marketplace_id=amazon_return.marketplace_id,
            )
            
            if refreshed_data:
                ret_data = refreshed_data.get('returnRequest', {})
                new_state = ret_data.get('returnRequestState')
                if new_state:
                    amazon_return.return_request_state = new_state
                    logger.info(f"[Upload] ✅ Updated return state to: {new_state}")
            
            self.db.flush()
            
            logger.info(f"[Upload] ========== UPLOAD COMPLETE: {amazon_return.return_request_id} ==========")
            return True
            
        except HTTPError as e:
            logger.error(f"[Upload] ❌ HTTP error at step '{step}' for {amazon_return.return_request_id}: {e}")
            amazon_return.mark_error(f"Upload failed at {step}: {str(e)}")
            label.state = LabelState.ERROR
            self.db.flush()
            raise
            
        except Exception as e:
            logger.error(f"[Upload] ❌ Error at step '{step}' for {amazon_return.return_request_id}: {e}")
            amazon_return.mark_error(f"Upload failed at {step}: {str(e)}")
            label.state = LabelState.ERROR
            self.db.flush()
            return False
            
    async def upload_all_pending(self) -> int:
        """
        Upload all pending labels to Amazon.
        
        Returns:
            Number of labels uploaded
        """
        # Get returns with generated labels that haven't been uploaded
        returns = (
            self.db.query(AmazonReturn)
            .filter(AmazonReturn.internal_status == InternalStatus.LABEL_GENERATED)
            .all()
        )
        
        logger.info(f"Uploading labels for {len(returns)} returns")
        
        uploaded_count = 0
        
        for amazon_return in returns:
            try:
                success = await self.upload_label(amazon_return)
                if success:
                    uploaded_count += 1
            except HTTPError:
                # HTTP errors are already logged and handled
                continue
            except Exception as e:
                logger.error(f"Error uploading label for {amazon_return.return_request_id}: {e}")
                amazon_return.mark_error(f"Upload error: {str(e)}")
                continue
                
        # Commit all changes
        self.db.commit()
        
        logger.info(f"Uploaded {uploaded_count} labels")
        return uploaded_count
