"""
JTL Service - Direct SQL Server queries for RMA and order details.

Runs locally on JTL server, no HTTP polling needed.
Adapted from ex_JTL-worker/worker.py OrderDataFetcher class.
"""
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from db.jtl_session import jtl_session
from config import settings

logger = logging.getLogger(__name__)


class JTLService:
    """
    Direct JTL SQL Server access for RMA and order details.
    
    Provides all the lookup functionality that was previously
    in the separate JTL worker, now integrated directly.
    """
    
    def __init__(self):
        self.jtl = jtl_session
    
    def _serialize_value(self, value: Any) -> Any:
        """Serialize value for JSON (handle datetime, etc.)."""
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        return value
    
    def _serialize_dict(self, d: Dict) -> Dict:
        """Serialize all values in dict for JSON."""
        return {k: self._serialize_value(v) for k, v in d.items()}
    
    def lookup_rma(self, order_id: str) -> Optional[str]:
        """
        Look up RMA number (AU number) in JTL system.
        
        Args:
            order_id: Amazon order ID (external order number)
            
        Returns:
            JTL AU number (cAuftragsNr) if found, None otherwise
        """
        query = """
            SELECT a.cAuftragsNr
            FROM Verkauf.tAuftrag a
            WHERE a.cExterneAuftragsnummer = ?
        """
        results = self.jtl.execute_query(query, (order_id,))
        
        if results and results[0].get('cAuftragsNr'):
            return results[0]['cAuftragsNr'].strip()
        return None
    
    def get_general_order_details(self, order_id: str) -> List[Dict[str, Any]]:
        """Retrieves general order details like buyer info, status, and items."""
        sql = """
        SELECT 
            Amz.cOrderId,
            Amz.dPurchaseDate,
            Amz.nStatus,
            Amz.cOrderStatus,
            Amz.nFBA,
            Amz.nPrime,
            Amz.cBuyerName,
            Amz.cBuyerEmail,
            Auftrag.cAuftragsNr AS Internal_Order_Number,
            Pos.cArtNr AS SKU,
            Pos.cName AS Product_Name,
            Listing.cASIN1 AS ASIN,
            Pos.nQuantityPurchased
        FROM 
            dbo.pf_amazon_bestellung AS Amz
        JOIN 
            dbo.pf_amazon_bestellungpos AS Pos ON Amz.kAmazonBestellung = Pos.kAmazonBestellung
        LEFT JOIN 
            Verkauf.tAuftrag AS Auftrag ON Auftrag.cExterneAuftragsnummer = Amz.cOrderId
        LEFT JOIN 
            dbo.pf_amazon_plattform AS Platt ON Platt.cMarketPlaceId = Amz.cMarketplaceId
        LEFT JOIN 
            dbo.pf_amazon_angebot AS Listing ON Listing.cSellerSKU = Pos.cArtNr
                                            AND Listing.kUser = Amz.kUser 
                                            AND Listing.nPlattform = Platt.kPlattform
        WHERE 
            Amz.cOrderId = ?
        """
        results = self.jtl.execute_query(sql, (order_id,))
        return [self._serialize_dict(r) for r in results]
    
    def get_product_descriptions(self, order_id: str) -> List[Dict[str, Any]]:
        """Retrieves product descriptions, manufacturer info, and identifiers."""
        sql = """
        SELECT 
            Pos.cArtNr AS SKU,
            Art.kArtikel AS Internal_ID,
            Desc_Wawi.cName AS Display_Name,
            Desc_Wawi.cBeschreibung AS Description_HTML,
            Desc_Wawi.cKurzBeschreibung AS Short_Description,
            Hersteller.cName AS Manufacturer,
            Art.cBarcode AS EAN,
            Art.cHAN AS MPN
        FROM 
            dbo.pf_amazon_bestellung AS Amz
        JOIN 
            dbo.pf_amazon_bestellungpos AS Pos ON Amz.kAmazonBestellung = Pos.kAmazonBestellung
        JOIN 
            dbo.tArtikel AS Art ON Art.cArtNr = Pos.cArtNr
        LEFT JOIN 
            dbo.tHersteller AS Hersteller ON Art.kHersteller = Hersteller.kHersteller
        LEFT JOIN 
            dbo.tArtikelBeschreibung AS Desc_Wawi ON Desc_Wawi.kArtikel = Art.kArtikel
                                                AND Desc_Wawi.kPlattform = 1 
                                                AND Desc_Wawi.kSprache = 1
        WHERE 
            Amz.cOrderId = ?
        """
        results = self.jtl.execute_query(sql, (order_id,))
        return [self._serialize_dict(r) for r in results]
    
    def get_product_specs(self, order_id: str) -> List[Dict[str, Any]]:
        """Retrieves product characteristics (Merkmale)."""
        sql = """
        SELECT 
            Pos.cArtNr AS SKU,
            MerkLang.cName AS Spec_Name,
            WertLang.cWert AS Spec_Value
        FROM 
            dbo.pf_amazon_bestellung AS Amz
        JOIN 
            dbo.pf_amazon_bestellungpos AS Pos ON Amz.kAmazonBestellung = Pos.kAmazonBestellung
        JOIN 
            dbo.tArtikel AS Art ON Art.cArtNr = Pos.cArtNr
        JOIN 
            dbo.tArtikelMerkmal AS ArtMerk ON ArtMerk.kArtikel = Art.kArtikel
        JOIN 
            dbo.tMerkmalSprache AS MerkLang ON MerkLang.kMerkmal = ArtMerk.kMerkmal
                                            AND MerkLang.kSprache = 1
        JOIN 
            dbo.tMerkmalWertSprache AS WertLang ON WertLang.kMerkmalWert = ArtMerk.kMerkmalWert
                                                AND WertLang.kSprache = 1
        WHERE 
            Amz.cOrderId = ?
        """
        results = self.jtl.execute_query(sql, (order_id,))
        return [self._serialize_dict(r) for r in results]
    
    def get_product_attributes(self, order_id: str) -> List[Dict[str, Any]]:
        """Retrieves product attributes."""
        sql = """
        SELECT 
            Pos.cArtNr AS SKU,
            AttrLang.cName AS Attribute_Name,
            ValLang.cWertVarchar AS Attribute_Value
        FROM 
            dbo.pf_amazon_bestellung AS Amz
        JOIN 
            dbo.pf_amazon_bestellungpos AS Pos ON Amz.kAmazonBestellung = Pos.kAmazonBestellung
        JOIN 
            dbo.tArtikel AS Art ON Art.cArtNr = Pos.cArtNr
        JOIN 
            dbo.tArtikelAttribut AS ArtAttr ON ArtAttr.kArtikel = Art.kArtikel
        JOIN 
            dbo.tAttributSprache AS AttrLang ON AttrLang.kAttribut = ArtAttr.kAttribut
                                            AND AttrLang.kSprache = 1
        JOIN 
            dbo.tArtikelAttributSprache AS ValLang ON ValLang.kArtikelAttribut = ArtAttr.kArtikelAttribut
                                                AND ValLang.kSprache = 1
        WHERE 
            Amz.cOrderId = ?
            AND AttrLang.cName != 'TPMS Hinweise'
        """
        results = self.jtl.execute_query(sql, (order_id,))
        return [self._serialize_dict(r) for r in results]
    
    def get_tracking_info(self, order_id: str) -> List[Dict[str, Any]]:
        """Retrieves tracking details."""
        sql = """
        SELECT 
            Versand.cIdentCode AS Tracking_Number,
            Versand.kLogistik AS Carrier,
            Versand.dVersendet AS Shipped_Date,
            Lieferschein.dErstellt AS DeliveryNote_Date,
            Auftrag.cAuftragsNr
        FROM 
            Verkauf.tAuftrag AS Auftrag
        JOIN 
            dbo.tLieferschein AS Lieferschein ON Lieferschein.kBestellung = Auftrag.kAuftrag
        JOIN 
            dbo.tVersand AS Versand ON Versand.kLieferschein = Lieferschein.kLieferschein
        WHERE 
            Auftrag.cExterneAuftragsnummer = ?
        """
        results = self.jtl.execute_query(sql, (order_id,))
        return [self._serialize_dict(r) for r in results]
    
    def get_all_order_data(self, order_id: str) -> Dict[str, Any]:
        """
        Fetch all order data from JTL in one call.
        
        Args:
            order_id: Amazon order ID
            
        Returns:
            Dict with:
            - internal_rma: RMA number if found
            - general_details: Order info
            - product_descriptions: Product descriptions
            - product_specs: Specifications
            - product_attributes: Attributes
            - tracking_info: Tracking details
        """
        logger.info(f"  Fetching all data for order: {order_id}")
        
        data = {
            "internal_rma": self.lookup_rma(order_id),
            "general_details": self.get_general_order_details(order_id),
            "product_descriptions": self.get_product_descriptions(order_id),
            "product_specs": self.get_product_specs(order_id),
            "product_attributes": self.get_product_attributes(order_id),
            "tracking_info": self.get_tracking_info(order_id),
        }
        
        logger.info(f"    RMA: {data['internal_rma'] or 'NOT_FOUND'}")
        logger.info(f"    General: {len(data['general_details'])}, Desc: {len(data['product_descriptions'])}, "
                    f"Specs: {len(data['product_specs'])}, Attrs: {len(data['product_attributes'])}, "
                    f"Tracking: {len(data['tracking_info'])}")
        
        return data
    
    def test_connection(self) -> bool:
        """Test JTL database connection."""
        return self.jtl.test_connection()


# Global singleton
jtl_service = JTLService()
