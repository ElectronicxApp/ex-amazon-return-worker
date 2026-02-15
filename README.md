# Amazon Return Worker

Standalone worker service for Amazon return processing.
Runs on the JTL server with direct access to JTL SQL Server.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment:
```bash
cp .env.example .env
# Edit .env with PostgreSQL and JTL credentials
```

3. Run worker:
```bash
python worker.py
```

## Architecture

- Polls PostgreSQL queue for `PROCESS_RETURNS` events
- Connects directly to JTL SQL Server for RMA/order lookups
- Processes returns: fetch → RMA lookup → labels → upload → DHL tracking

## DHL Tracking

The worker includes an integrated DHL tracking step (Step 9) that runs as part of each processing cycle.
All tracking logic is encapsulated in `services/dhl_tracking_service.py` — the flow file simply calls `tracking_service.update_all_tracking()`.

- **Batch tracking**: Groups up to 20 tracking numbers per API call (semicolon-separated piece-codes via `d-get-piece-detail`)
- **Tracks** all returns with Amazon label `carrier_tracking_id` that haven't been delivered or expired
- **Updates** tracking status and events timeline from DHL XML API
- **Signatures**: Retrieves proof-of-delivery signatures via `d-get-signature` XML API (hex-encoded GIF, stored as binary)
- **States**: `active` → `delivered` / `exception` / `expired` (max 90 days)
- **Fallback**: If batch API call fails, falls back to individual queries automatically
- **Always enabled**: Tracking runs every cycle when DHL credentials are configured (no on/off toggle)

### Configuration (environment variables)

| Variable | Description |
| :--- | :--- |
| `DHL_USERNAME` | DHL API username (appname) |
| `DHL_PASSWORD` | DHL API password |
| `DHL_CLIENT_ID` | DHL API client ID (HTTP Basic auth) |
| `DHL_CLIENT_SECRET` | DHL API client secret (HTTP Basic auth) |

### Constants (hardcoded, not configurable)

- `MAX_TRACKING_AGE_DAYS = 90` — stop tracking after 90 days
- `BATCH_SIZE = 20` — DHL d-get-piece-detail batch limit

### Database Tables

- `dhl_tracking_data` — Main tracking record per return
- `dhl_tracking_events` — Event timeline per tracking number (raw ICE/RIC codes)
- `dhl_tracking_signatures` — Proof of delivery signatures (binary GIF)

### ICE/RIC Event Code Enrichment (Frontend Only)

The backend stores only raw `ice_code` and `ric_code` values. Enrichment with human-readable descriptions, categories, and icons is done exclusively in the frontend via `ex-frontend/src/constants/dhlEventCodes.ts` (~270 mappings).

This keeps the backend schema simple and avoids storing redundant denormalized data.