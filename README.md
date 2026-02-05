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
- Processes returns: fetch → RMA lookup → labels → upload