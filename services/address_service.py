"""
Address Service - Fetches and stores shipping addresses from routing details.
"""

import asyncio
import logging
from typing import Optional
from datetime import datetime

from sqlalchemy.orm import Session

from models.amazon_return import AmazonReturn, AmazonReturnAddress, InternalStatus

logger = logging.getLogger(__name__)


class AddressService:
    """Service for fetching and storing customer addresses."""
    
    def __init__(self, db: Session, amazon_client):
        self.db = db
        self.client = amazon_client
        
    async def fetch_address(self, return_obj: AmazonReturn) -> bool:
        """
        Fetch address for a single return.
        Returns True if successful.
        """
        # Extract all required parameters from raw_data
        raw_data = return_obj.raw_data or {}
        
        # Get routing details from Amazon API
        api_call_params = {
            "order_id": return_obj.order_id,
            "return_request_id": return_obj.return_request_id,
            "marketplace_id": return_obj.marketplace_id or "",
            "merchant_party_id": raw_data.get("merchantPartyId", ""),
            "return_address_id": raw_data.get("returnAddressId", ""),
            "return_request_date": raw_data.get("returnRequestDate", 0),
            "shipping_address_id": raw_data.get("shippingAddressId", ""),
        }
        
        routing_data = await self.client.get_routing_details(**api_call_params)
        
        if not routing_data:
            logger.warning(f"No routing details for {return_obj.order_id}")
            return False
            
        # Parse shipFromAddress
        ship_from = routing_data.get("shipFromAddress")
        if not ship_from:
            logger.warning(f"No shipFromAddress in routing details for {return_obj.order_id}")
            return False
            
        # Create or update address
        address = self.db.query(AmazonReturnAddress).filter(
            AmazonReturnAddress.return_id == return_obj.id
        ).first()
        
        if not address:
            address = AmazonReturnAddress(return_id=return_obj.id)
            self.db.add(address)
            
        # Update address fields - map Amazon API fields to our model
        address.name = ship_from.get("name")
        address.address_field_one = ship_from.get("addressFieldOne")
        address.address_field_two = ship_from.get("addressFieldTwo")
        address.address_field_three = ship_from.get("addressFieldThree")
        address.district_or_county = ship_from.get("districtOrCounty")
        address.city = ship_from.get("city")
        address.state_or_region = ship_from.get("stateOrRegion")
        address.postal_code = ship_from.get("postalCode")
        address.country_code = ship_from.get("countryCode", "DE")
        address.phone_number = ship_from.get("phoneNumber")
        address.updated_at = datetime.utcnow()
        
        self.db.commit()
        logger.info(f"Address fetched for {return_obj.order_id}")
        return True
            
    async def fetch_all_missing_addresses(self) -> int:
        """
        Fetch addresses for all returns that don't have one.
        Returns count of addresses fetched.
        """
        # Get returns without addresses
        returns_without_address = self.db.query(AmazonReturn).filter(
            ~AmazonReturn.address.has(),
        ).all()
        
        logger.info(f"Found {len(returns_without_address)} returns without addresses")
        
        fetched = 0
        for amazon_return in returns_without_address:
            address = await self.fetch_address(amazon_return)
            if address:
                fetched += 1
                
            # Small delay to avoid rate limiting
            await asyncio.sleep(1)
            
        logger.info(f"Fetched {fetched} addresses")
        return fetched
