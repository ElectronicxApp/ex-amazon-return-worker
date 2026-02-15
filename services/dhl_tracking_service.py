"""
DHL Tracking Service - Fetches and stores DHL tracking data for return shipments.

Uses the DHL d-get-piece-detail XML API for tracking and d-get-signature for
proof-of-delivery signatures.

Key design decisions:
- ICE/RIC code enrichment is frontend-only (constants/dhlEventCodes.ts).
  The backend stores raw ice_code/ric_code and the frontend resolves descriptions.
- Batch queries: up to 20 tracking numbers per API call (semicolon-separated).
- Signatures use d-get-signature XML API. Returned as hex-encoded GIF in XML.
  Converted to binary bytes for DB storage.
- Tracking is always enabled. Max tracking age is a constant 90 days.
- Only amazon_label carrier_tracking_id is queried (Amazon updates it after upload).
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List


import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
from sqlalchemy.orm import Session


from models.amazon_return import AmazonReturn, AmazonReturnLabel
from models.dhl_tracking import DHLTrackingData, DHLTrackingEvent, DHLTrackingSignature
from config import settings


logger = logging.getLogger(__name__)

# Max age in days before tracking is considered expired
MAX_TRACKING_AGE_DAYS = 90

# Max tracking numbers per batch API call (DHL limit)
BATCH_SIZE = 20

# ICE codes indicating delivery
DELIVERED_ICE_CODES = {"DLVRD", "PCKDU"}
# ICE codes indicating exceptions
EXCEPTION_ICE_CODES = {"NTDEL", "SRTED", "RETUR", "RSTRY"}


class DHLTrackingService:
    """
    Service for fetching and persisting DHL tracking data.

    All tracking orchestration lives here. The return_flow only calls
    ``update_all_tracking()`` — no tracking logic in the flow file.

    Usage:
        service = DHLTrackingService(db)
        summary = service.update_all_tracking()
    """

    BASE_URL = "https://api-eu.dhl.com/parcel/de/tracking/v0/shipments"

    def __init__(self, db: Session):
        self.db = db
        self.username = settings.DHL_USERNAME
        self.password = settings.DHL_PASSWORD
        self.api_key = settings.DHL_CLIENT_ID
        self.api_secret = settings.DHL_CLIENT_SECRET

        if not all([self.username, self.password, self.api_key, self.api_secret]):
            logger.warning("DHL API credentials not fully configured — tracking will be disabled")
            self._enabled = False
        else:
            self._enabled = True

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ------------------------------------------------------------------
    # Public: full orchestration
    # ------------------------------------------------------------------

    def update_all_tracking(self) -> Dict[str, Any]:
        """
        Main entry point: update tracking for all eligible returns.

        1. Query returns with amazon_label carrier_tracking_id that are still active.
        2. Collect tracking numbers, skip already-stopped ones.
        3. Batch-fetch from DHL in groups of 20.
        4. Process & store results; fetch signatures for delivered DE shipments.
        5. Commit and return summary.

        Returns:
            Summary dict with counts and status.
        """
        summary: Dict[str, Any] = {
            "status": "success",
            "total": 0,
            "updated": 0,
            "failed": 0,
            "skipped": 0,
            "batch_api_calls": 0,
        }

        if not self._enabled:
            summary["status"] = "not_configured"
            return summary

        # Step 1: get return_ids with active tracking needs
        return_ids = self._get_returns_to_track()
        summary["total"] = len(return_ids)
        logger.info(f"Found {len(return_ids)} returns to track")

        if not return_ids:
            return summary

        # Step 2: collect tracking numbers per return
        return_tracking_map: Dict[str, int] = {}  # tracking_number -> return_id
        for rid in return_ids:
            amazon_return = (
                self.db.query(AmazonReturn)
                .filter(AmazonReturn.id == rid)
                .first()
            )
            if not amazon_return:
                summary["skipped"] += 1
                continue

            tn = None
            if amazon_return.amazon_label and amazon_return.amazon_label.carrier_tracking_id:
                tn = amazon_return.amazon_label.carrier_tracking_id

            if not tn:
                summary["skipped"] += 1
                continue

            # Check if we should still track this
            existing = (
                self.db.query(DHLTrackingData)
                .filter(DHLTrackingData.tracking_number == tn)
                .first()
            )
            if existing and not self._should_track(existing):
                summary["skipped"] += 1
                continue

            return_tracking_map[tn] = rid

        trackable = list(return_tracking_map.keys())
        logger.info(f"Trackable shipments: {len(trackable)} (skipped: {summary['skipped']})")

        # Step 3 & 4: batch-fetch and process
        for i in range(0, len(trackable), BATCH_SIZE):
            batch = trackable[i : i + BATCH_SIZE]
            summary["batch_api_calls"] += 1
            logger.info(f"Batch {summary['batch_api_calls']}: fetching {len(batch)} tracking numbers")

            try:
                batch_results = self._fetch_tracking_batch(batch)
            except Exception as exc:
                logger.error(f"Batch fetch error: {exc}")
                summary["failed"] += len(batch)
                continue

            for tn, api_response in batch_results.items():
                rid = return_tracking_map.get(tn)
                if not rid:
                    continue
                try:
                    if api_response is None:
                        summary["failed"] += 1
                        continue

                    ok = self._process_tracking_data(tn, rid, api_response)
                    if ok:
                        summary["updated"] += 1
                        self._maybe_fetch_signature(tn)
                    else:
                        summary["failed"] += 1
                except Exception as exc:
                    logger.error(f"Tracking error for {tn} (return {rid}): {exc}")
                    summary["failed"] += 1

        self.db.commit()
        logger.info(
            f"Tracking update complete: {summary['updated']} updated, "
            f"{summary['failed']} failed, {summary['skipped']} skipped, "
            f"{summary['batch_api_calls']} API calls (batch of {BATCH_SIZE})"
        )
        return summary

    # ------------------------------------------------------------------
    # Public: single-return update (kept for ad-hoc / API use)
    # ------------------------------------------------------------------

    def update_tracking_for_return(self, return_id: int) -> Dict[str, Any]:
        """
        Update tracking for a single return (used by API endpoints).
        """
        result: Dict[str, Any] = {"return_id": return_id, "status": "skipped", "tracking_number": None}

        amazon_return = (
            self.db.query(AmazonReturn)
            .filter(AmazonReturn.id == return_id)
            .first()
        )
        if not amazon_return:
            result["status"] = "not_found"
            return result

        tracking_number = None
        if amazon_return.amazon_label and amazon_return.amazon_label.carrier_tracking_id:
            tracking_number = amazon_return.amazon_label.carrier_tracking_id

        if not tracking_number:
            result["status"] = "no_tracking_number"
            return result

        result["tracking_number"] = tracking_number

        existing = (
            self.db.query(DHLTrackingData)
            .filter(DHLTrackingData.tracking_number == tracking_number)
            .first()
        )
        if existing and not self._should_track(existing):
            result["status"] = "tracking_stopped"
            result["tracking_state"] = existing.tracking_state
            return result

        api_response = self._fetch_tracking_info(tracking_number)
        if not api_response:
            result["status"] = "api_error"
            return result

        ok = self._process_tracking_data(tracking_number, return_id, api_response)
        if not ok:
            result["status"] = "processing_error"
            return result

        td = (
            self.db.query(DHLTrackingData)
            .filter(DHLTrackingData.tracking_number == tracking_number)
            .first()
        )
        result["status"] = "updated"
        result["tracking_state"] = td.tracking_state if td else "unknown"

        self._maybe_fetch_signature(tracking_number)
        return result

    # ------------------------------------------------------------------
    # DHL API: d-get-piece-detail (single)
    # ------------------------------------------------------------------

    def _fetch_tracking_info(self, tracking_number: str) -> Optional[Dict]:
        """Fetch tracking data for a single shipment."""
        if not self._enabled:
            return None

        xml_payload = (
            f'<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
            f'<data appname="{self.username}" language-code="de" '
            f'password="{self.password}" piece-code="{tracking_number}" '
            f'request="d-get-piece-detail"/>'
        )

        try:
            response = requests.get(
                self.BASE_URL,
                headers=self._tracking_headers(),
                params={"xml": xml_payload},
                auth=HTTPBasicAuth(self.api_key, self.api_secret),
                timeout=30,
            )
            response.encoding = "utf-8"

            if response.status_code != 200:
                logger.error(f"DHL API error for {tracking_number}: HTTP {response.status_code}")
                return None

            return self._parse_single_response(response.text)

        except requests.exceptions.RequestException as exc:
            logger.error(f"DHL API request failed for {tracking_number}: {exc}")
            return None

    # ------------------------------------------------------------------
    # DHL API: d-get-piece-detail (batch — up to 20)
    # ------------------------------------------------------------------

    def _fetch_tracking_batch(self, tracking_numbers: List[str]) -> Dict[str, Optional[Dict]]:
        """
        Fetch tracking data for up to 20 shipments in one API call.

        Semicolon-separated piece-codes.
        Falls back to individual queries on failure.
        """
        if not self._enabled:
            return {tn: None for tn in tracking_numbers}
        if not tracking_numbers:
            return {}

        batch = tracking_numbers[:BATCH_SIZE]
        results: Dict[str, Optional[Dict]] = {tn: None for tn in batch}

        piece_code_str = ";".join(batch)
        xml_payload = (
            f'<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
            f'<data appname="{self.username}" language-code="de" '
            f'password="{self.password}" piece-code="{piece_code_str}" '
            f'request="d-get-piece-detail"/>'
        )

        try:
            response = requests.get(
                self.BASE_URL,
                headers=self._tracking_headers(),
                params={"xml": xml_payload},
                auth=HTTPBasicAuth(self.api_key, self.api_secret),
                timeout=60,
            )
            response.encoding = "utf-8"

            if response.status_code != 200:
                logger.error(f"DHL batch API error: HTTP {response.status_code}")
                return self._fallback_individual(batch)

            parsed = self._parse_batch_response(response.text)
            if parsed is None:
                return self._fallback_individual(batch)

            for tn, data in parsed.items():
                if tn in results:
                    results[tn] = data

            # Fill gaps with individual queries
            missing = [tn for tn, v in results.items() if v is None]
            if missing:
                logger.info(f"Batch missing {len(missing)} results, trying individually")
                for tn in missing:
                    results[tn] = self._fetch_tracking_info(tn)

            return results

        except requests.exceptions.RequestException as exc:
            logger.error(f"DHL batch API request failed: {exc}")
            return self._fallback_individual(batch)

    # ------------------------------------------------------------------
    # DHL API: d-get-signature
    # ------------------------------------------------------------------

    def _fetch_signature(self, tracking_number: str) -> Optional[bytes]:
        """
        Retrieve proof-of-delivery signature via d-get-signature XML API.

        The response contains hex-encoded GIF image data in the ``image`` attribute.
        Two hex chars = one byte.  We decode to binary for DB storage.

        Per DHL docs:
        - Only works for delivered shipments (delivery-event-flag=1) with dest-country=DE.
        - Signatures should be retrieved once and stored locally.
        - ``request="d-get-signature"`` with ``piece-code`` attribute.
        """
        if not self._enabled:
            return None

        xml_payload = (
            f'<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
            f'<data appname="{self.username}" '
            f'password="{self.password}" '
            f'request="d-get-signature" '
            f'piece-code="{tracking_number}"/>'
        )

        try:
            response = requests.get(
                self.BASE_URL,
                headers=self._tracking_headers(),
                params={"xml": xml_payload},
                auth=HTTPBasicAuth(self.api_key, self.api_secret),
                timeout=30,
            )
            response.encoding = "utf-8"

            if response.status_code != 200:
                logger.warning(f"Signature API error for {tracking_number}: HTTP {response.status_code}")
                return None

            return self._parse_signature_response(response.text, tracking_number)

        except requests.exceptions.RequestException as exc:
            logger.error(f"Signature request failed for {tracking_number}: {exc}")
            return None

    # ------------------------------------------------------------------
    # XML parsing helpers
    # ------------------------------------------------------------------

    def _parse_single_response(self, xml_string: str) -> Optional[Dict]:
        """Parse DHL XML response for a single piece-shipment."""
        try:
            root = ET.fromstring(xml_string)
        except ET.ParseError as exc:
            logger.error(f"Error parsing DHL XML: {exc}")
            return None

        result: Dict[str, Any] = {
            "request_id": root.get("request-id"),
            "response_code": root.get("code"),
        }

        if root.attrib.get("code", "0") != "0":
            result["error"] = f"API Error Code: {root.attrib['code']}"

        shipment_list = root if root.get("name") == "piece-shipment-list" else None
        if shipment_list is None:
            shipment_list = next(
                (el for el in root.findall("data") if el.get("name") == "piece-shipment-list"),
                None,
            )
        if shipment_list is None:
            return result

        piece = next(
            (el for el in shipment_list.findall("data") if el.get("name") == "piece-shipment"),
            None,
        )
        if piece is None:
            return result

        result["shipment"] = self._extract_shipment_data(piece)
        result["events"] = self._extract_events(piece)
        return result

    def _parse_batch_response(self, xml_string: str) -> Optional[Dict[str, Dict]]:
        """Parse DHL XML response with multiple piece-shipment entries."""
        try:
            root = ET.fromstring(xml_string)
        except ET.ParseError as exc:
            logger.error(f"Error parsing DHL batch XML: {exc}")
            return None

        shipment_list = root if root.get("name") == "piece-shipment-list" else None
        if shipment_list is None:
            shipment_list = next(
                (el for el in root.findall("data") if el.get("name") == "piece-shipment-list"),
                None,
            )
        if shipment_list is None:
            return None

        results: Dict[str, Dict] = {}
        for piece in shipment_list.findall("data"):
            if piece.get("name") != "piece-shipment":
                continue
            piece_code = piece.get("piece-code")
            if not piece_code:
                continue
            results[piece_code] = {
                "request_id": root.get("request-id"),
                "response_code": root.get("code"),
                "shipment": self._extract_shipment_data(piece),
                "events": self._extract_events(piece),
            }
        return results if results else None

    def _parse_signature_response(self, xml_string: str, tracking_number: str) -> Optional[bytes]:
        """
        Parse d-get-signature XML response.

        Expected structure::

            <data name="signaturelist" code="0" request-id="...">
                <data name="signature"
                      event-date="11.03.2012"
                      mime-type="image/gif"
                      image="4749463..." />
            </data>

        The ``image`` attribute is hex-encoded GIF bytes (two chars per byte).
        """
        try:
            root = ET.fromstring(xml_string)
        except ET.ParseError as exc:
            logger.error(f"Error parsing signature XML for {tracking_number}: {exc}")
            return None

        if root.attrib.get("code", "0") != "0":
            logger.warning(f"Signature API returned code {root.attrib.get('code')} for {tracking_number}")
            return None

        # Find <data name="signature" ...>
        # Note: DHL uses "signature-list" (with hyphen) as the root element name
        sig_element = None
        if root.get("name") in ("signature-list", "signaturelist"):
            sig_element = next(
                (el for el in root.findall("data") if el.get("name") == "signature"),
                None,
            )
        else:
            for child in root.findall("data"):
                if child.get("name") in ("signature-list", "signaturelist"):
                    sig_element = next(
                        (el for el in child.findall("data") if el.get("name") == "signature"),
                        None,
                    )
                    break

        if sig_element is None:
            logger.warning(f"No signature element found for {tracking_number}")
            return None

        hex_image = sig_element.get("image")
        if not hex_image:
            logger.warning(f"Empty signature image for {tracking_number}")
            return None

        # Convert hex string to bytes: two hex chars -> one byte
        try:
            image_bytes = bytes.fromhex(hex_image)
            if len(image_bytes) < 10:
                logger.warning(f"Signature image too small for {tracking_number}: {len(image_bytes)} bytes")
                return None
            return image_bytes
        except ValueError as exc:
            logger.error(f"Error decoding signature hex for {tracking_number}: {exc}")
            return None

    @staticmethod
    def _extract_shipment_data(piece) -> Dict:
        """Extract shipment-level data from a piece-shipment XML element."""
        return {
            "piece_code": piece.get("piece-code"),
            "status": piece.get("status"),
            "short_status": piece.get("short-status"),
            "status_timestamp": piece.get("status-timestamp"),
            "ice": piece.get("ice"),
            "ric": piece.get("ric"),
            "delivery_event_flag": piece.get("delivery-event-flag"),
            "recipient_id": piece.get("recipient-id"),
            "recipient_id_text": piece.get("recipient-id-text"),
            "product_code": piece.get("product-code"),
            "product_name": piece.get("product-name"),
            "dest_country": piece.get("dest-country"),
            "origin_country": piece.get("origin-country"),
            "event_location": piece.get("event-location"),
            "event_country": piece.get("event-country"),
            "standard_event_code": piece.get("standard-event-code"),
        }

    @staticmethod
    def _extract_events(piece) -> list:
        """Extract events list from a piece-shipment XML element."""
        event_list = next(
            (el for el in piece.findall("data") if el.get("name") == "piece-event-list"),
            None,
        )
        events = []
        if event_list is not None:
            for idx, ev in enumerate(event_list.findall("data")):
                if ev.get("name") == "piece-event":
                    events.append({
                        "timestamp": ev.get("event-timestamp"),
                        "status": ev.get("event-status"),
                        "short_status": ev.get("event-short-status"),
                        "ice": ev.get("ice"),
                        "ric": ev.get("ric"),
                        "standard_event_code": ev.get("standard-event-code"),
                        "location": ev.get("event-location"),
                        "country": ev.get("event-country"),
                        "sequence": idx,
                    })
        return events

    # ------------------------------------------------------------------
    # DB persistence
    # ------------------------------------------------------------------

    def _process_tracking_data(self, tracking_number: str, return_id: int, api_response: Dict) -> bool:
        """
        Store/update tracking data & events in database.

        No ICE/RIC enrichment here — that is frontend-only via dhlEventCodes.ts.
        """
        try:
            shipment = api_response.get("shipment")
            if not shipment:
                logger.warning(f"No shipment data in API response for {tracking_number}")
                return False

            tracking_data = (
                self.db.query(DHLTrackingData)
                .filter(DHLTrackingData.tracking_number == tracking_number)
                .first()
            )

            delivery_flag = int(shipment.get("delivery_event_flag") or 0)
            status_ts = self._parse_timestamp(shipment.get("status_timestamp"))
            ice_code = shipment.get("ice")

            if tracking_data is None:
                tracking_data = DHLTrackingData(
                    tracking_number=tracking_number,
                    return_id=return_id,
                    tracking_started_at=datetime.utcnow(),
                )
                self.db.add(tracking_data)

            # Update shipment-level fields
            tracking_data.current_status = shipment.get("status")
            tracking_data.current_short_status = shipment.get("short_status")
            tracking_data.current_ice_code = ice_code
            tracking_data.current_ric_code = shipment.get("ric")
            tracking_data.last_update_timestamp = status_ts
            tracking_data.delivery_flag = delivery_flag
            tracking_data.recipient_id = shipment.get("recipient_id")
            tracking_data.recipient_id_text = shipment.get("recipient_id_text")
            tracking_data.product_code = shipment.get("product_code")
            tracking_data.product_name = shipment.get("product_name")
            tracking_data.dest_country = shipment.get("dest_country")
            tracking_data.origin_country = shipment.get("origin_country")
            tracking_data.updated_at = datetime.utcnow()

            # Determine tracking state
            new_state = self._determine_tracking_state(
                ice_code, delivery_flag, tracking_data.tracking_started_at
            )
            if new_state == "delivered":
                tracking_data.delivery_date = status_ts or datetime.utcnow()
                tracking_data.tracking_stopped_at = datetime.utcnow()
            elif new_state == "expired":
                tracking_data.tracking_stopped_at = datetime.utcnow()
            tracking_data.tracking_state = new_state

            # Replace events
            self.db.query(DHLTrackingEvent).filter(
                DHLTrackingEvent.tracking_number == tracking_number
            ).delete()

            for evt in api_response.get("events", []):
                self.db.add(DHLTrackingEvent(
                    tracking_number=tracking_number,
                    event_timestamp=self._parse_timestamp(evt.get("timestamp")),
                    event_status=evt.get("status"),
                    event_short_status=evt.get("short_status"),
                    ice_code=evt.get("ice"),
                    ric_code=evt.get("ric"),
                    standard_event_code=evt.get("standard_event_code"),
                    event_location=evt.get("location"),
                    event_country=evt.get("country"),
                    event_sequence=evt.get("sequence", 0),
                ))

            self.db.flush()
            return True

        except Exception as exc:
            logger.error(f"Error processing tracking data for {tracking_number}: {exc}", exc_info=True)
            return False

    def _maybe_fetch_signature(self, tracking_number: str) -> None:
        """Fetch & store signature if the shipment is delivered to DE and not yet retrieved."""
        td = (
            self.db.query(DHLTrackingData)
            .filter(DHLTrackingData.tracking_number == tracking_number)
            .first()
        )
        if not td:
            return
        if td.tracking_state != "delivered":
            return
        if td.signature_retrieved or td.signature_retrieval_failed:
            return
        if not td.dest_country or td.dest_country.upper() != "DE":
            return

        sig_bytes = self._fetch_signature(tracking_number)
        if sig_bytes:
            sig = (
                self.db.query(DHLTrackingSignature)
                .filter(DHLTrackingSignature.tracking_number == tracking_number)
                .first()
            )
            if not sig:
                sig = DHLTrackingSignature(tracking_number=tracking_number)
                self.db.add(sig)
            sig.signature_image = sig_bytes
            sig.signature_date = td.delivery_date
            sig.retrieved_at = datetime.utcnow()
            sig.mime_type = "image/gif"
            sig.retrieval_failed = False
            td.signature_retrieved = True
        else:
            td.signature_retrieval_failed = True

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def _get_returns_to_track(self) -> List[int]:
        """
        Query returns that need tracking updates.

        Only queries amazon_label carrier_tracking_id — after label upload,
        Amazon updates the return with the DHL tracking number.
        No batch_size limit; we process all eligible returns each cycle.
        """
        rows = (
            self.db.query(AmazonReturn.id)
            .join(AmazonReturnLabel, AmazonReturn.id == AmazonReturnLabel.return_id)
            .outerjoin(
                DHLTrackingData,
                AmazonReturnLabel.carrier_tracking_id == DHLTrackingData.tracking_number,
            )
            .filter(
                AmazonReturnLabel.carrier_tracking_id.isnot(None),
                (DHLTrackingData.tracking_number.is_(None)) | (DHLTrackingData.tracking_state == "active"),
            )
            .all()
        )
        return [rid for (rid,) in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _tracking_headers(self) -> Dict[str, str]:
        return {
            "DHL-API-Key": self.api_key,
            "Content-Type": "application/xml",
            "Accept": "application/xml",
        }

    def _fallback_individual(self, tracking_numbers: List[str]) -> Dict[str, Optional[Dict]]:
        """Fallback: query each tracking number individually."""
        logger.warning(f"Falling back to individual queries for {len(tracking_numbers)} numbers")
        return {tn: self._fetch_tracking_info(tn) for tn in tracking_numbers}

    @staticmethod
    def _should_track(tracking_data: DHLTrackingData) -> bool:
        """Return False if tracking should stop."""
        if tracking_data.tracking_state in ("delivered", "expired"):
            return False
        if tracking_data.tracking_stopped_at is not None:
            return False
        age = datetime.utcnow() - tracking_data.tracking_started_at
        if age.days > MAX_TRACKING_AGE_DAYS:
            return False
        return True

    @staticmethod
    def _determine_tracking_state(ice_code: Optional[str], delivery_flag: int, started_at: datetime) -> str:
        """Determine tracking state from ICE code, delivery flag, and age."""
        if ice_code and ice_code.upper() in DELIVERED_ICE_CODES:
            return "delivered"
        if ice_code and ice_code.upper() in EXCEPTION_ICE_CODES:
            return "exception"
        if delivery_flag and int(delivery_flag) == 1:
            return "delivered"
        age = datetime.utcnow() - started_at
        if age.days > MAX_TRACKING_AGE_DAYS:
            return "expired"
        return "active"

    @staticmethod
    def _parse_timestamp(ts_str: Optional[str]) -> Optional[datetime]:
        """Parse DHL timestamp formats."""
        if not ts_str:
            return None
        for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(ts_str, fmt)
            except ValueError:
                continue
        return None
