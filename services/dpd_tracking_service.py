"""
DPD Tracking Service - Fetches and stores DPD tracking data for return shipments.

Uses the DPD public tracking REST API (tracking.dpd.de) for parcel status and events.
No API credentials required for the public endpoint.

Key design decisions:
- Same DB tables as DHL (dhl_tracking_data/events/signatures) with carrier='DPD'.
- Same state machine: active → delivered/expired/exception/no_data.
- DPD statusCode mapped to ice_code column; scanType.name to standard_event_code.
- DPD public API returns one parcel per request (no batch).
- Proof of delivery: DPD provides a POD URL in the delivered event's links.
  Stored in pod_url on dhl_tracking_signatures (no binary image).
- Max tracking age: 60 days (same as DHL).
- First API failure → state becomes 'no_data' (immediate, no retries).
- Events are updated incrementally (no delete-and-replace).
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Set, Tuple

import requests
from sqlalchemy.orm import Session
from sqlalchemy import func

from models.amazon_return import AmazonReturn, AmazonReturnLabel
from models.dhl_tracking import DHLTrackingData, DHLTrackingEvent, DHLTrackingSignature
from config import settings


logger = logging.getLogger(__name__)

# Max age in days before tracking is considered expired (same as DHL)
MAX_TRACKING_AGE_DAYS = 60

# DPD status codes indicating delivery (from DPD docs section 6.1.4)
# statusCode "13" = delivered (to consignee or returned to sender)
# "DODEY" = picked up from pickup point by consignee
# "DEYY" with prevStatusCode "13" or "03" = delivered
DPD_DELIVERED_STATUS_CODES = {"13", "DODEY"}
DPD_DELIVERED_SCAN_NAMES = {"SC_13_DELIVERED"}

# DPD status codes indicating delivery issues / exceptions
# "04" = delivery failed, returned to terminal
# "08" = stopped in terminal, additional action needed
# "14" = not delivered, scanned by courier returning to terminal
DPD_EXCEPTION_STATUS_CODES = {"04", "08", "14"}

# DPD high-level status strings from the public API
DPD_DELIVERED_STATUSES = {"DELIVERED"}
DPD_EXCEPTION_STATUSES = set()  # Not used for public API (statusInfo uses different keys)


class DPDTrackingService:
    """
    Service for fetching and persisting DPD tracking data.

    Uses the same DB tables as DHL (with carrier='DPD') and the same
    state machine (active/delivered/expired/exception/no_data).

    Usage:
        service = DPDTrackingService(db)
        summary = service.update_all_tracking()
    """

    # DPD Public Tracking API (Germany, no auth required)
    BASE_URL = "https://tracking.dpd.de/rest/plc/de_DE"

    def __init__(self, db: Session):
        self.db = db
        # DPD public API needs no credentials
        self._enabled = True

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ------------------------------------------------------------------
    # Public: full orchestration
    # ------------------------------------------------------------------

    def update_all_tracking(self) -> Dict[str, Any]:
        """
        Main entry point: update tracking for all eligible DPD returns.

        Same flow as DHL:
        1. Query returns with DPD carrier_tracking_id that are still active.
        2. Fetch tracking data per parcel (DPD public API: one at a time).
        3. Process & store results; extract POD URLs for delivered shipments.
        4. Commit and return summary.
        """
        summary: Dict[str, Any] = {
            "status": "success",
            "carrier": "DPD",
            "total": 0,
            "updated": 0,
            "failed": 0,
            "no_data": 0,
            "skipped": 0,
            "api_calls": 0,
        }

        if not self._enabled:
            summary["status"] = "not_configured"
            return summary

        # Step 1: get return_ids with active DPD tracking needs
        return_ids = self._get_returns_to_track()
        summary["total"] = len(return_ids)
        logger.info(f"Found {len(return_ids)} DPD returns to track")

        if not return_ids:
            return summary

        # Step 2: collect tracking numbers per return
        return_tracking_map: Dict[str, int] = {}  # tracking_number -> return_id
        duplicate_tracking_numbers = 0

        for rid in return_ids:
            amazon_return = (
                self.db.query(AmazonReturn)
                .filter(AmazonReturn.id == rid)
                .first()
            )
            if not amazon_return:
                summary["skipped"] += 1
                continue

            if not amazon_return.amazon_label or not amazon_return.amazon_label.carrier_tracking_id:
                summary["skipped"] += 1
                continue

            # Secondary DPD guard
            carrier = (amazon_return.amazon_label.carrier_name or "").strip().upper()
            if not carrier.startswith("DPD"):
                summary["skipped"] += 1
                continue

            tn = amazon_return.amazon_label.carrier_tracking_id

            # Check if we should still track this (return_id-scoped)
            existing = (
                self.db.query(DHLTrackingData)
                .filter(
                    DHLTrackingData.tracking_number == tn,
                    DHLTrackingData.return_id == rid,
                )
                .first()
            )
            if existing and not self._should_track(existing):
                summary["skipped"] += 1
                continue

            if tn in return_tracking_map:
                duplicate_tracking_numbers += 1
                logger.debug(f"Duplicate DPD tracking number {tn}: return {rid}")
                continue

            return_tracking_map[tn] = rid

        trackable = list(return_tracking_map.keys())
        logger.info(
            f"DPD trackable: {len(trackable)} unique tracking numbers "
            f"(skipped: {summary['skipped']}, duplicates: {duplicate_tracking_numbers})"
        )

        # Step 3: fetch and process one at a time (DPD public API = one parcel per request)
        for tn in trackable:
            rid = return_tracking_map[tn]
            summary["api_calls"] += 1

            try:
                api_response = self._fetch_tracking_info(tn)

                if api_response is None:
                    self._mark_no_data(tn, rid)
                    summary["no_data"] += 1
                    continue

                ok = self._process_tracking_data(tn, rid, api_response)
                if ok:
                    summary["updated"] += 1
                    self._maybe_extract_pod(tn, rid)
                else:
                    summary["no_data"] += 1
            except Exception as exc:
                logger.error(f"DPD tracking error for {tn} (return {rid}): {exc}")
                summary["failed"] += 1

        self.db.commit()
        logger.info(
            f"DPD tracking complete: {summary['updated']} updated, "
            f"{summary['no_data']} no_data, {summary['failed']} failed, "
            f"{summary['skipped']} skipped, {summary['api_calls']} API calls"
        )
        return summary

    # ------------------------------------------------------------------
    # Public: single-return update
    # ------------------------------------------------------------------

    def update_tracking_for_return(self, return_id: int) -> Dict[str, Any]:
        """Update tracking for a single DPD return."""
        result: Dict[str, Any] = {"return_id": return_id, "status": "skipped", "tracking_number": None}

        amazon_return = (
            self.db.query(AmazonReturn)
            .filter(AmazonReturn.id == return_id)
            .first()
        )
        if not amazon_return:
            result["status"] = "not_found"
            return result

        if not amazon_return.amazon_label or not amazon_return.amazon_label.carrier_tracking_id:
            result["status"] = "no_tracking_number"
            return result

        carrier = (amazon_return.amazon_label.carrier_name or "").strip().upper()
        if not carrier.startswith("DPD"):
            result["status"] = "not_dpd_carrier"
            return result

        tracking_number = amazon_return.amazon_label.carrier_tracking_id
        result["tracking_number"] = tracking_number

        existing = (
            self.db.query(DHLTrackingData)
            .filter(
                DHLTrackingData.tracking_number == tracking_number,
                DHLTrackingData.return_id == return_id,
            )
            .first()
        )
        if existing and not self._should_track(existing):
            result["status"] = "tracking_stopped"
            result["tracking_state"] = existing.tracking_state
            return result

        api_response = self._fetch_tracking_info(tracking_number)
        if not api_response:
            self._mark_no_data(tracking_number, return_id)
            result["status"] = "no_data"
            return result

        ok = self._process_tracking_data(tracking_number, return_id, api_response)
        if not ok:
            result["status"] = "no_data"
            return result

        td = (
            self.db.query(DHLTrackingData)
            .filter(
                DHLTrackingData.tracking_number == tracking_number,
                DHLTrackingData.return_id == return_id,
            )
            .first()
        )
        result["status"] = "updated"
        result["tracking_state"] = td.tracking_state if td else "unknown"

        self._maybe_extract_pod(tracking_number, return_id)
        return result

    # ------------------------------------------------------------------
    # DPD API: public tracking endpoint
    # ------------------------------------------------------------------

    def _fetch_tracking_info(self, tracking_number: str) -> Optional[Dict]:
        """
        Fetch tracking data for a single DPD parcel via public REST API.

        Returns parsed response dict or None on failure.
        """
        url = f"{self.BASE_URL}/{tracking_number}"

        try:
            response = requests.get(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "DPD-Tracking-Service/1.0",
                },
                timeout=30,
            )

            if response.status_code != 200:
                logger.error(f"DPD API error for {tracking_number}: HTTP {response.status_code}")
                return None

            data = response.json()
            return self._parse_response(data, tracking_number)

        except requests.exceptions.RequestException as exc:
            logger.error(f"DPD API request failed for {tracking_number}: {exc}")
            return None
        except Exception as exc:
            logger.error(f"DPD response parse error for {tracking_number}: {exc}")
            return None

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_response(self, data: Dict, tracking_number: str) -> Optional[Dict]:
        """
        Parse DPD public API JSON response into our normalized format.

        Response structure:
            parcellifecycleResponse.parcelLifeCycleData.shipmentInfo  → shipment
            parcellifecycleResponse.parcelLifeCycleData.statusInfo[]  → high-level statuses
            parcellifecycleResponse.parcelLifeCycleData.scanInfo.scan[] → events
        """
        try:
            plc_response = data.get("parcellifecycleResponse", {})
            lifecycle = plc_response.get("parcelLifeCycleData", {})

            if not lifecycle:
                logger.warning(f"No parcelLifeCycleData in DPD response for {tracking_number}")
                return None

            shipment_info = lifecycle.get("shipmentInfo", {})
            status_info_list = lifecycle.get("statusInfo", [])
            scan_info = lifecycle.get("scanInfo", {})
            scans = scan_info.get("scan", [])

            # Extract current status from statusInfo (where isCurrentStatus=True)
            current_status = None
            current_status_label = None
            current_status_date = None
            delivery_flag = 0

            for si in status_info_list:
                if si.get("isCurrentStatus"):
                    current_status = si.get("status")  # e.g. "DELIVERED"
                    current_status_label = si.get("label")  # e.g. "Paket zugestellt"
                    current_status_date = si.get("date")  # e.g. "20.01.2026, 12:23"
                if si.get("status") == "DELIVERED" and si.get("statusHasBeenReached"):
                    delivery_flag = 1

            # Find the latest scan for the current ice_code equivalent
            latest_scan_code = None
            if scans:
                last_scan = scans[-1]
                scan_type = last_scan.get("scanData", {}).get("scanType", {})
                latest_scan_code = scan_type.get("code")

            # Extract shipment-level data
            shipment = {
                "piece_code": tracking_number,
                "status": current_status_label,
                "short_status": current_status,
                "status_timestamp": current_status_date,
                "ice": latest_scan_code,  # DPD statusCode → stored in ice_code column
                "ric": None,  # DPD has no RIC equivalent
                "delivery_event_flag": str(delivery_flag),
                "recipient_id": None,
                "recipient_id_text": None,
                "product_code": None,
                "product_name": shipment_info.get("productName"),
                "dest_country": shipment_info.get("receiverCountryIsoCode"),
                "origin_country": None,
            }

            # Extract service element code if available
            service_elements = shipment_info.get("serviceElements", [])
            if service_elements:
                shipment["product_code"] = service_elements[0].get("label")

            # Extract events from scanInfo
            events = []
            for idx, scan in enumerate(scans):
                scan_data = scan.get("scanData", {})
                scan_type = scan_data.get("scanType", {})
                scan_desc = scan.get("scanDescription", {})
                desc_content = scan_desc.get("content", [])

                # Extract additional codes as comma-separated string
                add_codes = scan.get("additionalCodes", [])
                add_code_str = ",".join(str(c) for c in add_codes) if add_codes else None

                # Extract service codes
                svc_elements = scan_data.get("serviceElements", [])
                svc_code_str = ",".join(se.get("code", "") for se in svc_elements) if svc_elements else None

                events.append({
                    "timestamp": scan.get("date"),  # ISO format: "2026-01-20T12:23:23"
                    "status": desc_content[0] if desc_content else None,
                    "short_status": scan_type.get("name"),  # e.g. "SC_13_DELIVERED"
                    "ice": scan_type.get("code"),  # DPD statusCode → ice_code column
                    "ric": svc_code_str,  # DPD serviceCode → ric_code column
                    "standard_event_code": scan_type.get("name"),  # e.g. "SC_13_DELIVERED"
                    "location": scan_data.get("location"),
                    "country": scan_data.get("country"),
                    "sequence": idx,
                    # DPD-specific: store add codes and POD links
                    "_add_codes": add_code_str,
                    "_links": scan.get("links", []),
                })

            return {
                "shipment": shipment,
                "events": events,
                "_status_info": status_info_list,  # kept for POD extraction
            }

        except Exception as exc:
            logger.error(f"Error parsing DPD response for {tracking_number}: {exc}", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # DB persistence
    # ------------------------------------------------------------------

    def _process_tracking_data(self, tracking_number: str, return_id: int, api_response: Dict) -> bool:
        """
        Store/update DPD tracking data & events in database.

        Same incremental approach as DHL — only new events are inserted.
        """
        try:
            shipment = api_response.get("shipment")
            if not shipment:
                logger.warning(f"No shipment data in DPD response for {tracking_number}")
                self._mark_no_data(tracking_number, return_id)
                return False

            tracking_data = (
                self.db.query(DHLTrackingData)
                .filter(
                    DHLTrackingData.tracking_number == tracking_number,
                    DHLTrackingData.return_id == return_id,
                )
                .first()
            )

            delivery_flag = int(shipment.get("delivery_event_flag") or 0)
            status_ts = self._parse_timestamp(shipment.get("status_timestamp"))
            status_code = shipment.get("ice")  # DPD statusCode stored in ice_code

            if tracking_data is None:
                first_event_ts = self._get_earliest_event_ts(api_response.get("events", []))
                tracking_data = DHLTrackingData(
                    tracking_number=tracking_number,
                    return_id=return_id,
                    carrier="DPD",
                    tracking_started_at=first_event_ts or datetime.utcnow(),
                )
                self.db.add(tracking_data)
                self.db.flush()

            # Update shipment-level fields
            tracking_data.current_status = shipment.get("status")
            tracking_data.current_short_status = shipment.get("short_status")
            tracking_data.current_ice_code = status_code
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
                status_code,
                shipment.get("short_status"),  # e.g. "DELIVERED"
                delivery_flag,
                tracking_data.tracking_started_at,
            )
            if new_state == "delivered":
                tracking_data.delivery_date = status_ts or datetime.utcnow()
                tracking_data.tracking_stopped_at = datetime.utcnow()
            elif new_state == "expired":
                tracking_data.tracking_stopped_at = datetime.utcnow()
            tracking_data.tracking_state = new_state

            # --- Incremental event update ---
            existing_events: Set[Tuple] = set()
            db_events = (
                self.db.query(DHLTrackingEvent)
                .filter(DHLTrackingEvent.tracking_data_id == tracking_data.id)
                .all()
            )
            for ev in db_events:
                key = (ev.event_timestamp, ev.ice_code, ev.ric_code, ev.event_sequence)
                existing_events.add(key)

            new_event_count = 0
            for evt in api_response.get("events", []):
                evt_ts = self._parse_timestamp(evt.get("timestamp"))
                evt_key = (evt_ts, evt.get("ice"), evt.get("ric"), evt.get("sequence", 0))
                if evt_key in existing_events:
                    continue
                self.db.add(DHLTrackingEvent(
                    tracking_data_id=tracking_data.id,
                    tracking_number=tracking_number,
                    event_timestamp=evt_ts,
                    event_status=evt.get("status"),
                    event_short_status=evt.get("short_status"),
                    ice_code=evt.get("ice"),
                    ric_code=evt.get("ric"),
                    standard_event_code=evt.get("standard_event_code"),
                    event_location=evt.get("location"),
                    event_country=evt.get("country"),
                    event_sequence=evt.get("sequence", 0),
                ))
                new_event_count += 1

            if new_event_count:
                logger.debug(f"Inserted {new_event_count} new DPD events for {tracking_number}")

            self.db.flush()
            return True

        except Exception as exc:
            logger.error(f"Error processing DPD tracking for {tracking_number}: {exc}", exc_info=True)
            return False

    def _mark_no_data(self, tracking_number: str, return_id: int) -> None:
        """Mark a DPD tracking record as 'no_data'."""
        tracking_data = (
            self.db.query(DHLTrackingData)
            .filter(
                DHLTrackingData.tracking_number == tracking_number,
                DHLTrackingData.return_id == return_id,
            )
            .first()
        )
        if tracking_data is None:
            tracking_data = DHLTrackingData(
                tracking_number=tracking_number,
                return_id=return_id,
                carrier="DPD",
                tracking_started_at=datetime.utcnow(),
            )
            self.db.add(tracking_data)

        tracking_data.tracking_state = "no_data"
        tracking_data.tracking_stopped_at = datetime.utcnow()
        tracking_data.updated_at = datetime.utcnow()
        self.db.flush()
        logger.info(f"Marked DPD {tracking_number} (return {return_id}) as no_data")

    def _maybe_extract_pod(self, tracking_number: str, return_id: int) -> None:
        """
        Extract proof-of-delivery URL from DPD tracking data.

        DPD provides POD as a URL link in the DELIVERED scan event.
        One attempt only.
        """
        td = (
            self.db.query(DHLTrackingData)
            .filter(
                DHLTrackingData.tracking_number == tracking_number,
                DHLTrackingData.return_id == return_id,
            )
            .first()
        )
        if not td:
            return
        if td.tracking_state != "delivered":
            return
        if td.signature_retrieved or td.signature_retrieval_failed:
            return

        # Find POD URL from the delivered event in scanInfo
        # The DELIVERED scan event has a link with target="DOCUMENT_POD_V2"
        pod_url = self._find_pod_url_from_events(tracking_number)

        if pod_url:
            sig = (
                self.db.query(DHLTrackingSignature)
                .filter(DHLTrackingSignature.tracking_data_id == td.id)
                .first()
            )
            if not sig:
                sig = DHLTrackingSignature(
                    tracking_data_id=td.id,
                    tracking_number=tracking_number,
                )
                self.db.add(sig)
            sig.pod_url = pod_url
            sig.signature_date = td.delivery_date
            sig.retrieved_at = datetime.utcnow()
            sig.mime_type = "application/pdf"  # DPD POD is typically a PDF document
            sig.retrieval_failed = False
            td.signature_retrieved = True
            logger.info(f"Extracted DPD POD URL for {tracking_number}")
        else:
            td.signature_retrieval_failed = True
            logger.debug(f"No POD URL found for DPD {tracking_number}")

    def _find_pod_url_from_events(self, tracking_number: str) -> Optional[str]:
        """
        Search the raw DPD API response for a POD URL.

        The DPD public API includes POD links in the delivered scan event:
        scan.links[] where target == "DOCUMENT_POD_V2"
        
        We re-fetch the tracking data to get fresh links (they may contain
        expiring tokens). This is the one-time POD retrieval.
        """
        try:
            url = f"{self.BASE_URL}/{tracking_number}"
            response = requests.get(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "DPD-Tracking-Service/1.0",
                },
                timeout=30,
            )
            if response.status_code != 200:
                return None

            data = response.json()
            plc = data.get("parcellifecycleResponse", {}).get("parcelLifeCycleData", {})
            scans = plc.get("scanInfo", {}).get("scan", [])

            for scan in scans:
                scan_type = scan.get("scanData", {}).get("scanType", {})
                if scan_type.get("code") == "13" or scan_type.get("name") == "SC_13_DELIVERED":
                    for link in scan.get("links", []):
                        if link.get("target") == "DOCUMENT_POD_V2":
                            pod_url = link.get("url")
                            if pod_url:
                                return pod_url

            return None

        except Exception as exc:
            logger.error(f"Error fetching DPD POD URL for {tracking_number}: {exc}")
            return None

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def _get_returns_to_track(self) -> List[int]:
        """
        Query returns that need DPD tracking updates.

        Filters:
        - Only DPD carriers (carrier_name ILIKE 'DPD%').
        - Only returns with a carrier_tracking_id set.
        - Only returns with no tracking record yet, or tracking_state == 'active'.
        """
        rows = (
            self.db.query(AmazonReturn.id)
            .join(AmazonReturnLabel, AmazonReturn.id == AmazonReturnLabel.return_id)
            .outerjoin(
                DHLTrackingData,
                AmazonReturn.id == DHLTrackingData.return_id,
            )
            .filter(
                AmazonReturnLabel.carrier_tracking_id.isnot(None),
                func.upper(AmazonReturnLabel.carrier_name).like("DPD%"),
                (DHLTrackingData.id.is_(None)) | (DHLTrackingData.tracking_state == "active"),
            )
            .all()
        )
        return [rid for (rid,) in rows]

    # ------------------------------------------------------------------
    # State helpers (same logic as DHL, different status codes)
    # ------------------------------------------------------------------

    @staticmethod
    def _should_track(tracking_data: DHLTrackingData) -> bool:
        """Return False if tracking should stop."""
        if tracking_data.tracking_state in ("delivered", "expired", "no_data"):
            return False
        if tracking_data.tracking_stopped_at is not None:
            return False
        age = datetime.utcnow() - tracking_data.tracking_started_at
        if age.days > MAX_TRACKING_AGE_DAYS:
            return False
        return True

    @staticmethod
    def _determine_tracking_state(
        status_code: Optional[str],
        high_level_status: Optional[str],
        delivery_flag: int,
        started_at: datetime,
    ) -> str:
        """
        Determine tracking state from DPD status code and high-level status.

        DPD status mapping:
        - statusCode "13" or high-level "DELIVERED" → delivered
        - statusCode "04", "08", "14" → exception
        - Age > 60 days → expired
        - Everything else → active
        """
        # Check high-level status first (from statusInfo)
        if high_level_status and high_level_status.upper() in DPD_DELIVERED_STATUSES:
            return "delivered"

        # Check DPD scan type code
        if status_code and status_code in DPD_DELIVERED_STATUS_CODES:
            return "delivered"

        if status_code and status_code in DPD_EXCEPTION_STATUS_CODES:
            return "exception"

        if delivery_flag and int(delivery_flag) == 1:
            return "delivered"

        age = datetime.utcnow() - started_at
        if age.days > MAX_TRACKING_AGE_DAYS:
            return "expired"

        return "active"

    @staticmethod
    def _parse_timestamp(ts_str: Optional[str]) -> Optional[datetime]:
        """
        Parse DPD timestamp formats.

        DPD uses:
        - ISO format in scan events: "2026-01-20T12:23:23"
        - German format in statusInfo: "20.01.2026, 12:23"
        """
        if not ts_str:
            return None
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",       # ISO from scanInfo.scan[].date
            "%d.%m.%Y, %H:%M",          # German from statusInfo[].date
            "%d.%m.%Y %H:%M",           # Without comma variant
            "%Y-%m-%d %H:%M:%S",        # Standard ISO
        ):
            try:
                return datetime.strptime(ts_str, fmt)
            except ValueError:
                continue
        return None

    @classmethod
    def _get_earliest_event_ts(cls, events: list) -> Optional[datetime]:
        """Return the earliest event timestamp from a list of event dicts."""
        if not events:
            return None
        timestamps = [cls._parse_timestamp(e.get("timestamp")) for e in events]
        valid = [ts for ts in timestamps if ts is not None]
        return min(valid) if valid else None
