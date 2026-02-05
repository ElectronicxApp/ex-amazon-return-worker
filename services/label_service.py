"""
Label Service - Generates DHL shipping labels.

Handles:
- Creating DHL return shipments via DHL Returns API
- Generating labels (PDF)
- Uploading to AWS S3 (no local storage)
- Updating AmazonReturnGeneratedLabel records
"""

import logging
import base64
from datetime import datetime
from typing import Optional, List

import requests
from sqlalchemy.orm import Session
import boto3
from botocore.exceptions import ClientError

from models.amazon_return import (
    AmazonReturn,
    AmazonReturnGeneratedLabel,
    InternalStatus,
    LabelState,
)
from config import settings

logger = logging.getLogger(__name__)


class DHLService:
    """
    DHL Returns API client for creating return shipments.
    
    Uses DHL OAuth ROPC (Resource Owner Password Credentials) flow
    for authentication and the DHL Returns API for label generation.
    """
    
    TOKEN_URL = "https://api-eu.dhl.com/parcel/de/account/auth/ropc/v1/token"
    RETURNS_URL = "https://api-eu.dhl.com/parcel/de/shipping/returns/v1/orders"
    
    def __init__(self):
        self.username = settings.DHL_USERNAME
        self.password = settings.DHL_PASSWORD
        self.client_id = settings.DHL_CLIENT_ID
        self.client_secret = settings.DHL_CLIENT_SECRET
        self.access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
    
    @staticmethod
    def get_receiver_id(country_code: str) -> str:
        """Get DHL receiver ID based on country code."""
        COUNTRY_MAPPING = {
            'DE': 'RetourenLager01',  # Germany
            'CH': 'che',              # Switzerland
            'IT': 'ita',              # Italy
            'FR': 'fra',              # France
            'GB': 'gbr',              # Great Britain
            'AT': 'aut',              # Austria
            'NL': 'nld',              # Netherlands
            'BE': 'bel',              # Belgium
            'ES': 'esp',              # Spain
            'PL': 'pol',              # Poland
            'CZ': 'cze',              # Czech Republic
            'SK': 'svk',              # Slovakia
            'HU': 'hun',              # Hungary
            'RO': 'rou',              # Romania
            'HR': 'hrv',              # Croatia
            'SI': 'svn',              # Slovenia
            'BG': 'bgr',              # Bulgaria
            'EE': 'est',              # Estonia
            'LV': 'lva',              # Latvia
            'LT': 'ltu',              # Lithuania
            'FI': 'fin',              # Finland
            'SE': 'swe',              # Sweden
            'DK': 'dnk',              # Denmark
            'IE': 'irl',              # Ireland
            'PT': 'prt',              # Portugal
            'GR': 'grc',              # Greece
            'CY': 'cyp',              # Cyprus
            'LU': 'lux',              # Luxembourg
            'MT': 'mlt',              # Malta
        }
        return COUNTRY_MAPPING.get(country_code, 'RetourenLager01')

        
    def _get_access_token(self) -> Optional[str]:
        """Get DHL OAuth access token using ROPC flow."""
        logger.info("[DHL AUTH] Requesting access token...")
        
        if not all([self.username, self.password, self.client_id, self.client_secret]):
            logger.error("[DHL AUTH] ❌ Missing DHL credentials in environment variables")
            return None
        
        data = {
            'grant_type': 'password',
            'username': self.username,
            'password': self.password,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        headers = {
            'accept': 'application/json',
            'content-type': 'application/x-www-form-urlencoded'
        }
        
        try:
            response = requests.post(self.TOKEN_URL, data=data, headers=headers, timeout=30)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data.get('access_token')
            logger.info(f"[DHL AUTH] ✅ Token obtained, expires in {token_data.get('expires_in')} seconds")
            return self.access_token
            
        except Exception as e:
            logger.error(f"[DHL AUTH] ❌ Failed to get token: {e}")
            return None
    
    def _ensure_token(self) -> bool:
        """Ensure we have a valid access token."""
        if not self.access_token:
            return self._get_access_token() is not None
        return True
        
    async def create_return_shipment(
        self,
        shipper_name: str,
        shipper_street: str,
        shipper_house: str,
        shipper_city: str,
        shipper_postal_code: str,
        shipper_country: str,
        rma_number: str,
        customs_items: Optional[List[dict]] = None,
    ) -> Optional[dict]:
        """Create a DHL return shipment and get label."""
        logger.info(f"[DHL] Creating return shipment for {shipper_name}")
        
        if not self._ensure_token():
            logger.error("[DHL] ❌ Failed to obtain DHL access token")
            return None
        
        # Build shipper data (customer returning the item)
        shipper_data = {
            "name1": shipper_name[:35],
            "addressStreet": shipper_street[:35],
            "addressHouse": (shipper_house[:10] if shipper_house else "1"),
            "city": shipper_city[:35],
            "postalCode": shipper_postal_code[:10],
            "itemWeight": {
                "uom": "kg",
               "value": 1
            },
        }
        
        # Build customs items if not provided
        if not customs_items:
            customs_items = [{
                "itemDescription": "Return Item",
                "packagedQuantity": 1,
                "itemValue": {"currency": "EUR", "value": 50},
                "itemWeight": {
                    "uom": "kg",
                    "value": 1
                },
            }]
        
        # Build DHL API payload
        payload = {
            "receiverId": DHLService.get_receiver_id(shipper_country),
            "shipper": shipper_data,
            "customsDetails": {
                "items": customs_items
            },
            "customerReference": rma_number[:35] if rma_number else "",
            "itemWeight": {
                "uom": "kg",
                "value": 1
            },
        }
        
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f"Bearer {self.access_token}"
        }
        
        logger.info(f"[DHL API] Calling {self.RETURNS_URL}")
        logger.info(f"[DHL API] Receiver ID: {payload['receiverId']}")
        logger.info(f"[DHL API] Shipper: {shipper_data['name1']}, {shipper_data['city']}")
        
        try:
            response = requests.post(self.RETURNS_URL, json=payload, headers=headers, timeout=30)

            logger.info(f"[DHL API] Response Status: {response.status_code}")
            
            if response.status_code in [200, 201]:
                data = response.json()
                label_b64 = data.get('label', {}).get('b64', '')
                
                result = {
                    "tracking_number": data.get("shipmentNo"),
                    "label_b64": label_b64,
                    "label_data": base64.b64decode(label_b64) if label_b64 else b"",
                    "international_tracking": data.get("internationalShipmentNo"),
                    "routing_code": data.get("routingCode"),
                    "status": "CREATED"
                }
                
                logger.info(f"[DHL] ✅ Label created: {result['tracking_number']}")
                return result
            else:
                logger.error(f"[DHL API] ❌ Error {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"[DHL API] ❌ Request failed: {e}")
            return None


class S3Service:
    """AWS S3 service for storing DHL labels."""
    
    def __init__(self):
        self.bucket_name = settings.S3_BUCKET_NAME
        self.region = settings.AWS_REGION
        self._client = None
        
    @property
    def client(self):
        """Lazy initialization of S3 client."""
        if self._client is None:
            self._client = boto3.client(
                's3',
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region_name=self.region
            )
        return self._client
    
    def upload_label(
        self,
        label_data: bytes,
        return_id: int,
        order_id: str,
        tracking_number: str,
    ) -> Optional[dict]:
        """Upload DHL label PDF to S3."""
        s3_key = f"amazon_dhl_label_of_returnID_{return_id}_and_orderId_{order_id}_with_tracking_number{tracking_number}.pdf"
        
        logger.info(f"[S3] Uploading label to s3://{self.bucket_name}/{s3_key}")
        
        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=label_data,
                ContentType='application/pdf'
            )
            
            logger.info(f"[S3] ✅ Label uploaded successfully: {s3_key}")
            
            return {
                "s3_bucket": self.bucket_name,
                "s3_key": s3_key
            }
            
        except ClientError as e:
            logger.error(f"[S3] ❌ Upload failed: {e}")
            return None
        except Exception as e:
            logger.error(f"[S3] ❌ Unexpected error: {e}")
            return None
    
    def get_label(self, s3_key: str) -> Optional[bytes]:
        """Download label from S3."""
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body'].read()
        except Exception as e:
            logger.error(f"[S3] ❌ Download failed: {e}")
            return None


class LabelService:
    """Service for generating and managing DHL return labels."""
    
    def __init__(self, db: Session):
        self.db = db
        self.dhl_service = DHLService()
        self.s3_service = S3Service()
        
    def _parse_address(self, address_line: str) -> tuple:
        """Parse address line into street and house number."""
        if not address_line:
            return ("", "1")  # DHL requires house number - use fallback
            
        # Split from right, max 1 split - house number is usually at the end
        parts = address_line.rsplit(' ', 1)
        if len(parts) == 2 and any(c.isdigit() for c in parts[1]):
            return (parts[0], parts[1])
        else:
            # No house number detected - use whole line as street and fallback house number
            return (address_line, "1")
        
    async def generate_label(self, amazon_return: AmazonReturn) -> Optional[AmazonReturnGeneratedLabel]:
        """Generate a DHL return label for a single return."""
        if not amazon_return.address:
            logger.error(f"Return {amazon_return.return_request_id} has no address")
            amazon_return.mark_error("No address available for label generation")
            return None
            
        address = amazon_return.address
        
        # Validate required address fields
        if not address.name or not address.city or not address.postal_code:
            logger.error(f"Return {amazon_return.return_request_id} has incomplete address")
            amazon_return.mark_error("Incomplete address: missing name, city, or postal code")
            return None
        
        try:
            # Parse street address
            address_line = address.address_field_two or address.address_field_one or address.address_field_three or ""
            street, house = self._parse_address(address_line)
            postal_code = address.postal_code
            
            # Build customs items from return items
            customs_items = []
            if amazon_return.items:
                for item in amazon_return.items:
                    price = item.calculated_price or item.unit_price or 0
                    customs_items.append({
                        "itemDescription": (item.product_title or "Return Item")[:50],
                        "packagedQuantity": item.return_quantity or 1,
                        "itemValue": {"currency": str(amazon_return.currency_code or "EUR"), "value": float(price) if price > 0 else 0.0}
                    })
            
            if not customs_items:
                customs_items = None  # Let DHLService use default
            
            # Use RMA number or order_id as reference
            rma_reference = amazon_return.internal_rma or amazon_return.rma_id or amazon_return.order_id
            
            logger.info(f"[Label] Generating DHL label for {amazon_return.return_request_id}")
            logger.info(f"[Label] Customer: {address.name}, {address.city} {postal_code}")
            logger.info(f"[Label] RMA Reference: {rma_reference}")
            
            # Step 1: Create DHL shipment and get label
            result = await self.dhl_service.create_return_shipment(
                shipper_name=address.name,
                shipper_street=street,
                shipper_house=house,
                shipper_city=address.city,
                shipper_postal_code=postal_code,
                shipper_country=address.country_code or "DE",
                rma_number=rma_reference,
                customs_items=customs_items,
            )
            
            if not result or not result.get("label_data"):
                logger.error(f"DHL shipment creation failed for {amazon_return.return_request_id}")
                amazon_return.mark_error("DHL shipment creation failed - no label returned")
                return None
                
            tracking_number = result["tracking_number"]
            label_data = result["label_data"]
            
            logger.info(f"[Label] ✅ DHL tracking: {tracking_number}")
            
            # Step 2: Upload label PDF to S3
            s3_result = self.s3_service.upload_label(
                label_data=label_data,
                return_id=amazon_return.return_request_id,
                order_id=amazon_return.order_id,
                tracking_number=tracking_number,
            )
            
            if not s3_result:
                logger.error(f"S3 upload failed for {amazon_return.return_request_id}")
                amazon_return.mark_error("S3 label upload failed")
                return None
                
            # Step 3: Check for existing label record or create new
            existing = self.db.query(AmazonReturnGeneratedLabel).filter(
                AmazonReturnGeneratedLabel.return_id == amazon_return.id
            ).first()
            
            if existing:
                label = existing
            else:
                label = AmazonReturnGeneratedLabel(return_id=amazon_return.id)
                self.db.add(label)
                
            # Step 4: Update label record
            label.tracking_number = tracking_number
            label.s3_key = s3_result["s3_key"]
            label.state = LabelState.CREATED
            
            # Step 5: Update return status
            amazon_return.internal_status = InternalStatus.LABEL_GENERATED
            amazon_return.last_error = None
            
            self.db.flush()
            
            logger.info(f"[Label] ✅ Label generated and uploaded for {amazon_return.return_request_id}: {tracking_number}")
            return label
            
        except Exception as e:
            logger.error(f"[Label] ❌ Failed to generate label for {amazon_return.return_request_id}: {e}")
            amazon_return.mark_error(f"Label generation failed: {str(e)}")
            return None
            
    async def generate_labels_for_eligible(self) -> int:
        """Generate labels for all eligible returns."""
        # Get returns that need labels
        returns = (
            self.db.query(AmazonReturn)
            .filter(AmazonReturn.internal_status.in_([
                InternalStatus.ELIGIBLE
            ]))
            .all()
        )
        
        logger.info(f"Generating labels for {len(returns)} returns")
        
        generated_count = 0
        
        for amazon_return in returns:
            try:
                label = await self.generate_label(amazon_return)
                if label:
                    generated_count += 1
            except Exception as e:
                logger.error(f"Error generating label for {amazon_return.return_request_id}: {e}")
                amazon_return.mark_error(f"Label generation error: {str(e)}")
                continue
                
        # Commit all changes
        self.db.commit()
        
        logger.info(f"Generated {generated_count} labels")
        return generated_count
