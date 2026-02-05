"""
Amazon Client - HTTP API wrapper for Seller Central.

This client uses browser automation via Playwright to maintain session
and interact with Amazon Seller Central. The worker runs on a Windows
server with GUI availability.
"""

import asyncio
import logging
import time
import html
import json as json_lib
import re
from typing import Optional, Any, List

import requests

from config import settings

logger = logging.getLogger(__name__)


class HTTPError(Exception):
    """Raised when HTTP request fails."""
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


class SessionExpiredError(Exception):
    """Raised when Amazon session has expired (4xx errors)."""
    pass


class AmazonClient:
    """
    HTTP client for Amazon Seller Central APIs.
    
    Uses requests.Session for HTTP calls.
    """
    
    BASE_URL = "https://sellercentral.amazon.de"
    RETURNS_API = "/returns/api/listreturns-v2"
    ROUTING_API = "/returns/api/routingDetails"
    UPLOAD_LABEL_API = "/returns/api/uploadReturnLabel"
    ACTIONS_API = "/returns/manage-actions"
    S3_ARGS_API = "/returns/get-s3-arguments"
    ALEXANDRIA_ARGS_API = "/returns/get-alexandria-arguments-v2"
    COMPLETE_RETURN_API = "/returns/api/completeReturn"
    
    def __init__(self, session: Optional[requests.Session] = None):
        self._session = session or requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7",
        }
        
    @property
    def session(self) -> requests.Session:
        """Get current session."""
        return self._session
        
    def update_session(self, session: requests.Session):
        """Update the session after refresh."""
        self._session = session
        
    async def _make_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> requests.Response:
        """Make HTTP request with error handling."""
        if not self.session:
            raise SessionExpiredError("No session available")
            
        # Add default headers
        headers = kwargs.pop('headers', {})
        headers = {**self.headers, **headers}
        
        try:
            # Execute request in thread pool
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: getattr(self.session, method.lower())(url, headers=headers, **kwargs)
            )
            
            # Check for session expiry (4xx errors)
            if 400 <= response.status_code < 500:
                logger.warning(f"Got {response.status_code} - session expired")
                raise SessionExpiredError(f"Session expired (HTTP {response.status_code}).")
                    
            # Check for server errors (5xx)
            if response.status_code >= 500:
                raise HTTPError(f"Server error: {response.status_code}", response.status_code)
                    
            return response
            
        except requests.RequestException as e:
            raise HTTPError(f"Request failed: {e}")
    
    async def fetch_returns(
        self,
        page_size: int = 100,
        scroll_id: Optional[str] = None,
        days_back: int = 90,
    ) -> dict:
        """Fetch returns from Amazon API."""
        base_url = (
            f"{self.BASE_URL}{self.RETURNS_API}?"
            "aToZClaimFiled&cardType&communicationDeliveryId&createdAfter&createdBefore&keyword"
            "&marketplaceIds=APJ6JRA9NG5V4&marketplaceIds=A28R8C7NBKEWEA&marketplaceIds=A2NODRKZP88ZB9"
            "&marketplaceIds=A1C3SOZRARQ6R3&marketplaceIds=A13V1IB3VIYZZH&marketplaceIds=A33AVAJ2PDY3EV"
            "&marketplaceIds=AMEN7PMS3EDWL&marketplaceIds=A1805IZSGTT6HS&marketplaceIds=A1RKKUPIHCS9HS"
            "&marketplaceIds=A1PA6795UKMFR9&marketplaceIds=A1F83G8C2ARO7P"
            "&onPendingActionsTab=false&orderBy=CreatedDateDesc&pageSize=100"
            "&pendingActionsFilterBy="
            "&previousPageScrollId="
            "&returnRequestState="
            f"&selectedDateRange={days_back}"
        )
        
        url = f"{base_url}&scrollId={scroll_id}" if scroll_id else base_url
        
        logger.info("Fetching returns from Amazon API...")
        
        response = await self._make_request('GET', url)
        
        if response.status_code != 200:
            logger.error(f"API Error: {response.status_code} - {response.text[:500]}")
            raise HTTPError(f"Amazon API returned {response.status_code}", response.status_code)
            
        return response.json()
        
    async def fetch_all_returns(
        self,
        max_pages: int = 100,
        days_back: int = 90,
    ) -> List[dict]:
        """Fetch all returns using pagination."""
        all_returns = []
        scroll_id = None
        page = 0
        
        while page < max_pages:
            result = await self.fetch_returns(
                page_size=100,
                scroll_id=scroll_id,
                days_back=days_back,
            )
            
            returns = result.get("returnRequests", [])
            if not returns:
                break
                
            all_returns.extend(returns)
            logger.info(f"Fetched page {page + 1}: {len(returns)} returns (total: {len(all_returns)})")
            
            scroll_id = result.get("scrollId")
            if not scroll_id:
                break
                
            page += 1
            await asyncio.sleep(1)
            
        logger.info(f"Total returns fetched: {len(all_returns)}")
        return all_returns
        
    async def get_routing_details(
        self,
        order_id: str,
        return_request_id: str,
        marketplace_id: str,
        merchant_party_id: str,
        return_address_id: str,
        return_request_date: int,
        shipping_address_id: str,
    ) -> Optional[dict]:
        """Get routing details (shipping address) for an order."""
        try:
            url = (
                f"{self.BASE_URL}{self.ROUTING_API}?"
                f"marketplaceId={marketplace_id}"
                f"&merchantPartyId={merchant_party_id}"
                f"&orderId={order_id}"
                f"&returnAddressId={return_address_id}"
                f"&returnRequestDate={return_request_date}"
                f"&returnRequestId={return_request_id}"
                f"&shippingAddressId={shipping_address_id}"
            )
            
            logger.info(f"Fetching routing details for order {order_id}")
            
            response = await self._make_request('GET', url)
            
            if response.status_code != 200:
                logger.error(f"Failed to get routing details: {response.status_code} - {response.text[:200]}")
                return None
            
            return response.json()
        except SessionExpiredError:
            raise
        except Exception as e:
            logger.error(f"Failed to get routing details for {order_id}: {e}")
            return None
            
    async def get_return_details(
        self,
        return_request_id: str,
        marketplace_id: str,
    ) -> Optional[dict]:
        """Get detailed information for a specific return."""
        url = (
            f"{self.BASE_URL}{self.ACTIONS_API}"
            f"?loadFromEs=false&loadRmlUrl=true"
            f"&marketplaceId={marketplace_id}"
            f"&returnRequestId={return_request_id}&tabId=1"
        )
        
        try:
            response = await self._make_request('GET', url)
            
            if response.status_code == 200:
                return response.json()
            return None
            
        except Exception as e:
            logger.error(f"Failed to get return details: {e}")
            return None
            
    async def get_s3_bucket_args(
        self,
        return_request_id: str,
        marketplace_id: str,
    ) -> Optional[dict]:
        """Get arguments for S3 label upload."""
        url = (
            f"{self.BASE_URL}{self.S3_ARGS_API}"
            f"?marketplaceId={marketplace_id}"
            f"&returnRequestId={return_request_id}&tabId=1"
        )
        
        try:
            response = await self._make_request('GET', url)
            
            if response.status_code == 200:
                return response.json()
            return None
            
        except Exception as e:
            logger.error(f"Failed to get S3 args: {e}")
            return None
            
    async def get_alexandria_args(
        self,
        customer_id: str,
        marketplace_id: str,
        seller_id: str,
        return_request_id: str,
        csrf: str,
    ) -> Optional[dict]:
        """Get arguments for Alexandria document upload."""
        url = (
            f"{self.BASE_URL}{self.ALEXANDRIA_ARGS_API}"
            f"?customerId={customer_id}"
            f"&marketplaceId={marketplace_id}"
            f"&sellerId={seller_id}"
        )
        
        headers = {
            "Anti-Csrftoken-A2z": csrf,
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/gp/returns/list/v2/actions?returnRequestId={return_request_id}&marketplaceId={marketplace_id}&tabId=1",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty"
        }
        
        try:
            response = await self._make_request('GET', url, headers=headers)
            
            if response.status_code == 200:
                return response.json()
            return None
            
        except Exception as e:
            logger.error(f"Failed to get Alexandria args: {e}")
            return None
    
    async def alexandria_upload(
        self,
        alex_args: dict,
        label_png: bytes,
        customer_id: str,
        seller_id: str,
        marketplace_id: str,
        return_request_id: str,
        csrf: str,
    ) -> Optional[dict]:
        """Upload document to Alexandria service."""
        alexandria_args = alex_args.get('alexandriaArguments', {})
        upload_url = alexandria_args.get('Upload_URL')
        
        if not upload_url:
            logger.error("[Alexandria] No Upload_URL in alexandria args")
            return {"success": False, "doc_version_id": None}
        
        url = f"{self.BASE_URL}{upload_url}"
        
        files = {
            "_utf8_enable": (None, '&#10003'),
            "AX-SessionID": (None, alexandria_args.get('AX-SessionID', '')),
            "AX-DocumentDisposition": (None, alexandria_args.get('AX-DocumentDisposition', '')),
            "AX-Destination": (None, alexandria_args.get('AX-Destination', '')),
            "AX-OwnershipInfo::file::CustomerID": (None, customer_id),
            "AX-OwnershipInfo::file::SellerId": (None, seller_id),
            "AX-OwnershipInfo::file::MarketplaceId": (None, marketplace_id),
            "AX-Signature": (None, alexandria_args.get('AX-Signature', '')),
            "file": ("label.png", label_png, "image/png")
        }
        
        headers = {
            "Anti-Csrftoken-A2z": csrf,
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/gp/returns/list/v2/actions?returnRequestId={return_request_id}&marketplaceId={marketplace_id}&tabId=1",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty"
        }
        
        try:
            logger.info(f"[Alexandria] Uploading label to {url}")
            
            response = await self._make_request('POST', url, files=files, headers=headers)
            
            response_text = response.text
            logger.info(f"[Alexandria] Response status: {response.status_code}")
            
            if 'REQUEST_SUCCEEDED' in response_text:
                match = re.search(r'name="uploadResponse"\s+value="([^"]*)"', response_text)
                if match:
                    decoded = html.unescape(match.group(1))
                    try:
                        upload_data = json_lib.loads(decoded)
                        doc_id = upload_data.get('content', {}).get('documentUploadResponseList', {}).get('file', {}).get('content', {}).get('documentId')
                        if doc_id:
                            logger.info(f"[Alexandria] ✅ Upload successful. DocVersionId: {doc_id}")
                            return {"success": True, "doc_version_id": doc_id}
                    except Exception as parse_err:
                        logger.warning(f"[Alexandria] Could not parse uploadResponse JSON: {parse_err}")
                
                # Fallback: try XML pattern
                match = re.search(r'<docVersionId>([^<]+)</docVersionId>', response_text)
                if match:
                    doc_version_id = match.group(1)
                    logger.info(f"[Alexandria] ✅ Upload successful. DocVersionId: {doc_version_id}")
                    return {"success": True, "doc_version_id": doc_version_id}
                    
                logger.warning("[Alexandria] Upload succeeded but could not extract docVersionId")
                return {"success": True, "doc_version_id": None}
            
            logger.error(f"[Alexandria] ❌ Upload failed. Response: {response_text[:500]}")
            return {"success": False, "doc_version_id": None}
            
        except Exception as e:
            logger.error(f"[Alexandria] ❌ Upload error: {e}")
            return {"success": False, "doc_version_id": None}
            
    async def upload_to_s3(
        self,
        s3_args: dict,
        label_binary: bytes,
        key: str,
        csrf: str,
    ) -> bool:
        """Upload label to Amazon S3 bucket."""
        url = f"https://{s3_args['bucketName']}.s3.amazonaws.com"
        
        files = {"file": ("label.png", label_binary, "image/png")}
        data = {
            "acl": "bucket-owner-full-control",
            "AWSAccessKeyId": s3_args["accessKey"],
            "policy": s3_args["policy"],
            "signature": s3_args["signature"],
            "key": key,
            "success_action_redirect": "#",
            "x-amz-meta-applicationid": ""
        }
        
        headers = {
            "Anti-Csrftoken-A2z": csrf,
            "Origin": self.BASE_URL,
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": f"{self.BASE_URL}/",
        }
        
        try:
            response = await self._make_request('POST', url, data=data, files=files, headers=headers)
            
            success = response.status_code in [200, 201, 204]
            logger.info(f"S3 Upload Status: {response.status_code} - {'Success' if success else 'Failed'}")
            return success
            
        except Exception as e:
            logger.error(f"S3 upload failed: {e}")
            return False
            
    async def complete_return(
        self,
        return_request_id: str,
        marketplace_id: str,
        seller_id: str,
        document_id: str,
        address_id: str,
        tracking_id: str,
        csrf: str,
    ) -> bool:
        """Complete return by submitting label to Amazon."""
        url = f"{self.BASE_URL}/returns/update-return-request-v2"
        
        payload = {
            "labelDetails": {
                "carrierName": "DHL",
                "carrierTrackingId": tracking_id,
                "labelPrice": 0,
                "labelType": "MerchantPrePaidLabel",
                "currencyCode": "EUR"
            },
            "eventType": "SET_AUTHORIZATION_DETAILS",
            "merchantPartyId": seller_id,
            "marketplaceId": marketplace_id,
            "merchantRmaId": None,
            "returnRequestId": return_request_id,
            "merchantRML": return_request_id,
            "saveAsDefaultInstructions": False,
            "returnInstructions": None,
            "labelUploaded": True,
            "alexandriaDocVersionId": document_id,
            "activeTabId": "EDIT_AUTHORIZATION",
            "returnAddressId": address_id
        }
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7",
            "Anti-Csrftoken-A2z": csrf,
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/gp/returns/list/v2/actions?returnRequestId={return_request_id}&marketplaceId={marketplace_id}&tabId=1",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        }
        
        try:
            logger.info(f"[Complete] Submitting return {return_request_id} with tracking {tracking_id}")
            
            response = await self._make_request('POST', url, json=payload, headers=headers)
            
            logger.info(f"[Complete] Response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                success = result.get('succeeded', False)
                
                if success:
                    logger.info(f"[Complete] ✅ Return {return_request_id} completed successfully!")
                else:
                    error_msg = result.get('errorMessage', result.get('error', 'Unknown error'))
                    logger.error(f"[Complete] ❌ Return failed: {error_msg}")
                    
                return success
            else:
                logger.error(f"[Complete] ❌ HTTP {response.status_code}: {response.text[:500]}")
                return False
            
        except Exception as e:
            logger.error(f"[Complete] ❌ Error: {e}")
            return False
    
    async def refetch_return(
        self,
        return_request_id: str,
        marketplace_id: str,
    ) -> Optional[dict]:
        """Refetch a single return to get latest data after submission."""
        return await self.get_return_details(return_request_id, marketplace_id)
