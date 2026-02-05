"""
Amazon Return Worker - Standalone service for return processing.

Runs on the JTL server with direct access to:
- PostgreSQL (remote) - for return data and queue events
- JTL SQL Server (local) - for RMA and order details

Execution:
- Runs continuously, polling for queue events and executing at scheduled times
- Scheduled runs at times defined in WORKER_SCHEDULE_TIMES (default: 06:00,12:00,18:00)
- User-triggered runs via PROCESS_RETURNS queue events

Flow:
1. Poll PostgreSQL queue for PROCESS_RETURNS events
2. Fetch returns from Amazon (browser automation)
3. Look up RMA + order details from JTL (direct SQL)
4. Store order details in PostgreSQL
5. Filter eligible returns
6. Generate DHL labels
7. Upload labels to Amazon
8. Update statistics

Usage:
    python worker.py
"""

import asyncio
import signal
import logging
from datetime import datetime, time as dt_time
from typing import Optional, Tuple, Dict, Any, List

from config import settings

# Setup logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_schedule_times(schedule_str: str) -> List[dt_time]:
    """Parse comma-separated schedule times (HH:MM format)."""
    times = []
    for time_str in schedule_str.split(","):
        time_str = time_str.strip()
        if time_str:
            try:
                hour, minute = map(int, time_str.split(":"))
                times.append(dt_time(hour=hour, minute=minute))
            except (ValueError, IndexError):
                logger.warning(f"Invalid schedule time format: {time_str}")
    return sorted(times)


def get_next_scheduled_time(schedule_times: List[dt_time]) -> Optional[datetime]:
    """Get the next scheduled run time."""
    if not schedule_times:
        return None
    
    now = datetime.now()
    today = now.date()
    
    # Find next time today
    for t in schedule_times:
        scheduled = datetime.combine(today, t)
        if scheduled > now:
            return scheduled
    
    # All times today have passed, get first time tomorrow
    from datetime import timedelta
    tomorrow = today + timedelta(days=1)
    return datetime.combine(tomorrow, schedule_times[0])


class AmazonReturnWorker:
    """
    Standalone worker for Amazon return processing.
    
    Runs on JTL server with direct access to both PostgreSQL and JTL SQL Server.
    Supports both scheduled execution and queue-based triggering.
    """
    
    def __init__(self, poll_interval: int = None):
        self.poll_interval = poll_interval or settings.WORKER_POLL_INTERVAL
        self.running = True
        self.schedule_times = parse_schedule_times(settings.WORKER_SCHEDULE_TIMES)
        self._last_scheduled_run: Optional[datetime] = None
        
        # Import here to avoid circular imports
        from db.postgres_session import get_session, test_connection as pg_test
        from db.jtl_session import jtl_session
        
        self.get_session = get_session
        self.jtl = jtl_session
    
    def poll_queue(self) -> Tuple[Optional[int], Optional[Dict]]:
        """
        Poll PostgreSQL queue for pending PROCESS_RETURNS events.
        
        Returns:
            Tuple of (event_id, event_data) if found, (None, None) otherwise
        """
        from models.worker_queue import WorkerQueueEvent
        
        with self.get_session() as db:
            event = db.query(WorkerQueueEvent).filter(
                WorkerQueueEvent.status == "PENDING",
                WorkerQueueEvent.event_type == "PROCESS_RETURNS"
            ).order_by(WorkerQueueEvent.created_at).first()
            
            if event:
                event.status = "PROCESSING"
                event.started_at = datetime.utcnow()
                db.commit()
                
                logger.info(f"🔔 Picked up event {event.id} (triggered by {event.triggered_by})")
                return event.id, event.data
        
        return None, None
    
    def mark_complete(self, event_id: int, error: str = None, result: Dict = None):
        """Mark a queue event as completed or failed."""
        from models.worker_queue import WorkerQueueEvent
        
        if event_id == 0:
            # Scheduled run, no event to mark
            return
        
        with self.get_session() as db:
            event = db.query(WorkerQueueEvent).filter(
                WorkerQueueEvent.id == event_id
            ).first()
            
            if event:
                event.status = "FAILED" if error else "COMPLETED"
                event.completed_at = datetime.utcnow()
                event.error_message = error[:500] if error else None
                if result:
                    event.result = result
                db.commit()
                
                status = "❌ FAILED" if error else "✅ COMPLETED"
                logger.info(f"Event {event_id}: {status}")
    
    async def run_processing_cycle(self, days_back: int = None, event_id: int = 0) -> Dict[str, Any]:
        """
        Run a complete return processing cycle using ReturnFlow.
        
        Args:
            days_back: Number of days to fetch returns for
            event_id: Queue event ID (0 for scheduled runs)
            
        Returns:
            Cycle summary dict
        """
        days = days_back or settings.WORKER_DAYS_BACK
        
        logger.info("=" * 80)
        logger.info(f"Starting Return Processing Cycle")
        logger.info(f"Event ID: {event_id} {'(scheduled)' if event_id == 0 else '(triggered)'}")
        logger.info(f"Days to fetch: {days}")
        logger.info("=" * 80)
        
        try:
            from services.return_flow import ReturnFlow
            
            with self.get_session() as db:
                flow = ReturnFlow(db)
                result = await flow.run_cycle(days_back=days)
            
            self.mark_complete(event_id, result=result)
            
            logger.info("=" * 80)
            logger.info(f"Cycle Complete: {result.get('status', 'unknown')}")
            logger.info("=" * 80)
            
            return result
            
        except Exception as e:
            logger.error(f"Processing failed: {e}", exc_info=True)
            self.mark_complete(event_id, str(e))
            return {"status": "error", "error": str(e)}
    
    def should_run_scheduled(self) -> bool:
        """Check if it's time to run a scheduled cycle."""
        if not self.schedule_times:
            return False
        
        now = datetime.now()
        current_minute = dt_time(now.hour, now.minute)
        
        # Check if current time matches any schedule time
        for scheduled_time in self.schedule_times:
            if (current_minute.hour == scheduled_time.hour and 
                current_minute.minute == scheduled_time.minute):
                
                # Avoid running multiple times in the same minute
                if self._last_scheduled_run:
                    if (self._last_scheduled_run.hour == now.hour and 
                        self._last_scheduled_run.minute == now.minute and
                        self._last_scheduled_run.date() == now.date()):
                        return False
                
                return True
        
        return False
    
    def test_connections(self) -> bool:
        """Test all database connections."""
        from db.postgres_session import test_connection as pg_test
        
        logger.info("Testing database connections...")
        
        pg_ok = pg_test()
        jtl_ok = self.jtl.test_connection()
        
        if pg_ok and jtl_ok:
            logger.info("✅ All database connections successful")
            return True
        else:
            logger.error("❌ Database connection test failed")
            return False
    
    async def run(self):
        """
        Main worker loop - combines scheduled execution and queue polling.
        
        Runs at scheduled times AND when triggered via queue events.
        """
        logger.info("=" * 80)
        logger.info("Amazon Return Worker Starting")
        logger.info("=" * 80)
        logger.info(f"PostgreSQL: {settings.POSTGRES_URL.split('@')[1] if '@' in settings.POSTGRES_URL else 'configured'}")
        logger.info(f"JTL Server: {settings.JTL_SQL_SERVER}")
        logger.info(f"Poll Interval: {self.poll_interval}s")
        logger.info(f"Schedule Times: {', '.join(t.strftime('%H:%M') for t in self.schedule_times)}")
        
        next_scheduled = get_next_scheduled_time(self.schedule_times)
        if next_scheduled:
            logger.info(f"Next Scheduled Run: {next_scheduled.strftime('%Y-%m-%d %H:%M')}")
        
        logger.info("=" * 80)
        
        # Test connections on startup
        if not self.test_connections():
            logger.error("Exiting due to connection failure")
            return
        
        logger.info(f"Worker running. Polling every {self.poll_interval}s...")
        logger.info(f"Press Ctrl+C to stop")
        
        while self.running:
            try:
                # Check for queue events (user-triggered)
                event_id, data = self.poll_queue()
                
                if event_id:
                    days = data.get("days", settings.WORKER_DAYS_BACK) if data else settings.WORKER_DAYS_BACK
                    await self.run_processing_cycle(days_back=days, event_id=event_id)
                    continue  # Check for more events immediately
                
                # Check for scheduled run
                if self.should_run_scheduled():
                    logger.info("⏰ Scheduled run triggered!")
                    self._last_scheduled_run = datetime.now()
                    await self.run_processing_cycle(event_id=0)
                    
                    # Update next scheduled time
                    next_scheduled = get_next_scheduled_time(self.schedule_times)
                    if next_scheduled:
                        logger.info(f"Next Scheduled Run: {next_scheduled.strftime('%Y-%m-%d %H:%M')}")
                
                await asyncio.sleep(self.poll_interval)
                
            except KeyboardInterrupt:
                logger.info("Shutdown requested")
                self.running = False
            except Exception as e:
                logger.error(f"Worker error: {e}", exc_info=True)
                await asyncio.sleep(self.poll_interval)
        
        logger.info("Worker stopped")
    
    def stop(self):
        """Signal worker to stop."""
        self.running = False


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    

def main():
    """Entry point."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    worker = AmazonReturnWorker()
    
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception as e:
        logger.error(f"Worker failed: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
