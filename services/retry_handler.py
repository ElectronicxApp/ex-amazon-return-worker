"""
Retry Handler - Centralized retry logic with session reset and circuit breaker.

Features:
- Configurable retry attempts with exponential backoff
- Circuit breaker pattern for resilience
- Thread-safe with asyncio.Lock
- Structured error classification
- Session reset on recoverable errors

When any Amazon API call fails with 4xx/5xx:
1. Check circuit breaker state
2. Perform hard session reset
3. Wait with exponential backoff
4. Re-initialize session
5. Retry the API call up to MAX_ATTEMPTS times
6. If still fails, record error and exit flow for that return
"""

import asyncio
import logging
import time
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional, Tuple, TYPE_CHECKING, Set

from config import settings

if TYPE_CHECKING:
    from services.session_manager import SessionManager
    from services.amazon_client import AmazonClient
    from models.amazon_return import AmazonReturn

logger = logging.getLogger(__name__)


# Recoverable HTTP status codes that warrant retry
RECOVERABLE_STATUS_CODES: Set[int] = {401, 403, 429, 500, 502, 503, 504}


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "CLOSED"      # Normal operation, requests allowed
    OPEN = "OPEN"          # Failures exceeded threshold, requests blocked
    HALF_OPEN = "HALF-OPEN"  # Testing if service recovered


class CircuitBreaker:
    """
    Circuit breaker pattern for handling cascading failures.
    
    States:
    - CLOSED: Normal operation, all requests pass through
    - OPEN: Too many failures, block requests to prevent resource exhaustion
    - HALF-OPEN: Test with single request after reset timeout
    
    Usage:
        breaker = CircuitBreaker()
        if breaker.allow_request():
            try:
                result = await api_call()
                breaker.record_success()
            except Exception:
                breaker.record_failure()
    """
    
    def __init__(
        self,
        failure_threshold: int = None,
        reset_timeout: int = None
    ):
        self.failure_threshold = failure_threshold or settings.CIRCUIT_BREAKER_THRESHOLD
        self.reset_timeout = reset_timeout or settings.CIRCUIT_BREAKER_RESET_TIMEOUT
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()
        
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state
    
    async def allow_request(self) -> bool:
        """
        Check if request should be allowed.
        
        Returns:
            True if request is allowed, False if blocked
        """
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True
                
            if self._state == CircuitState.OPEN:
                # Check if reset timeout has passed
                if self._last_failure_time:
                    elapsed = time.time() - self._last_failure_time
                    if elapsed >= self.reset_timeout:
                        logger.info(f"[CircuitBreaker] Transitioning to HALF-OPEN after {elapsed:.0f}s")
                        self._state = CircuitState.HALF_OPEN
                        return True
                        
                logger.warning("[CircuitBreaker] Circuit OPEN - request blocked")
                return False
                
            # HALF-OPEN: Allow one test request
            return True
    
    async def record_success(self):
        """Record successful request - reset failure count."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                logger.info("[CircuitBreaker] Success in HALF-OPEN - closing circuit")
            
            self._failure_count = 0
            self._state = CircuitState.CLOSED
            self._last_failure_time = None
    
    async def record_failure(self):
        """Record failed request - potentially open circuit."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                logger.warning("[CircuitBreaker] Failure in HALF-OPEN - reopening circuit")
                self._state = CircuitState.OPEN
                return
            
            if self._failure_count >= self.failure_threshold:
                logger.warning(
                    f"[CircuitBreaker] Threshold reached ({self._failure_count}/{self.failure_threshold}) - opening circuit"
                )
                self._state = CircuitState.OPEN
    
    def get_status(self) -> dict:
        """Get circuit breaker status for monitoring."""
        return {
            "state": self._state.value,
            "failure_count": self._failure_count,
            "failure_threshold": self.failure_threshold,
            "reset_timeout": self.reset_timeout,
            "last_failure": datetime.fromtimestamp(self._last_failure_time).isoformat() if self._last_failure_time else None
        }


class SessionResetRequiredError(Exception):
    """Raised when a session reset and retry is needed."""
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(message)
        self.original_error = original_error


class RetryWithSessionReset:
    """
    Handler for retrying Amazon API calls with session reset.
    
    Features:
    - Configurable retry attempts from settings
    - Exponential backoff between retries
    - Circuit breaker integration
    - Thread-safe with asyncio.Lock
    - Structured error classification
    
    Usage:
        retry_handler = RetryWithSessionReset(session_manager, amazon_client)
        success, result = await retry_handler.execute_with_retry(
            api_call=amazon_client.fetch_returns,
            amazon_return=return_obj,  # Optional, for error recording
            days_back=90  # kwargs for the API call
        )
    """
    
    # Global circuit breaker shared across all instances
    _circuit_breaker = CircuitBreaker()
    
    def __init__(
        self, 
        session_manager: 'SessionManager',
        amazon_client: 'AmazonClient'
    ):
        self.session_manager = session_manager
        self.amazon_client = amazon_client
        self._retry_lock = asyncio.Lock()
        
    @classmethod
    def get_circuit_breaker(cls) -> CircuitBreaker:
        """Get the shared circuit breaker instance."""
        return cls._circuit_breaker
        
    async def execute_with_retry(
        self,
        api_call: Callable,
        amazon_return: Optional['AmazonReturn'] = None,
        error_context: str = "",
        *args, 
        **kwargs
    ) -> Tuple[bool, Any]:
        """
        Execute an API call with retries after session reset on failure.
        
        Args:
            api_call: Async callable to execute
            amazon_return: Optional AmazonReturn object for error recording
            error_context: Description of what operation is being performed
            *args, **kwargs: Arguments to pass to the API call
            
        Returns:
            Tuple of (success: bool, result: Any)
            - On success: (True, result_from_api_call)
            - On failure: (False, error_message)
        """
        # Check circuit breaker first
        if not await self._circuit_breaker.allow_request():
            error_msg = f"{error_context}: Circuit breaker OPEN - request blocked"
            logger.warning(f"[Retry] {error_msg}")
            return False, error_msg
        
        max_attempts = settings.RETRY_MAX_ATTEMPTS
        base_wait = settings.RETRY_WAIT_SECONDS
        backoff_multiplier = settings.RETRY_BACKOFF_MULTIPLIER
        
        last_error = None
        
        for attempt in range(max_attempts + 1):  # +1 for initial attempt
            try:
                if attempt == 0:
                    # First attempt - no wait
                    logger.debug(f"[Retry] Initial attempt for: {error_context}")
                else:
                    # Retry attempt - calculate wait with exponential backoff
                    wait_time = base_wait * (backoff_multiplier ** (attempt - 1))
                    logger.info(f"[Retry] Attempt {attempt}/{max_attempts} - waiting {wait_time:.0f}s...")
                    await asyncio.sleep(wait_time)
                
                # Execute the API call
                result = await api_call(*args, **kwargs)
                
                # Success - record and return
                await self._circuit_breaker.record_success()
                
                if attempt > 0:
                    logger.info(f"[Retry] ✅ Success on attempt {attempt}")
                    
                return True, result
                
            except Exception as e:
                last_error = e
                
                # Check if error is recoverable
                if not self._is_recoverable_error(e):
                    error_msg = f"{error_context}: {str(e)}"
                    logger.error(f"[Retry] Non-recoverable error: {error_msg}")
                    if amazon_return:
                        self._record_error(amazon_return, error_msg)
                    return False, error_msg
                
                logger.warning(f"[Retry] Attempt {attempt} failed: {e}")
                
                # Record failure for circuit breaker
                await self._circuit_breaker.record_failure()
                
                # If we have more attempts, perform session reset
                if attempt < max_attempts:
                    async with self._retry_lock:
                        await self._perform_session_reset()
        
        # All attempts exhausted
        error_msg = f"{error_context}: Failed after {max_attempts} retry attempts - {str(last_error)}"
        logger.error(f"[Retry] ❌ All retries exhausted: {error_msg}")
        
        if amazon_return:
            self._record_error(amazon_return, error_msg)
            
        # Notify placeholder for future implementation
        notify_user_of_failure(
            return_id=amazon_return.return_request_id if amazon_return else "unknown",
            error=error_msg
        )
        
        return False, error_msg
    
    async def _perform_session_reset(self):
        """Perform session reset with proper wait time."""
        logger.info("[Retry] Performing session reset...")
        
        # Step 1: Hard reset session
        logger.info("[Retry] Step 1: Hard reset session...")
        await self.session_manager.hard_reset()
        
        # Step 2: Wait for session to settle
        wait_time = settings.SESSION_RESET_WAIT_SECONDS
        logger.info(f"[Retry] Step 2: Waiting {wait_time}s for session reset cooldown...")
        await asyncio.sleep(wait_time)
        
        # Step 3: Re-initialize session
        logger.info("[Retry] Step 3: Re-initializing session with fresh login...")
        new_session = await self.session_manager.init_session_for_cycle()
        
        if not new_session:
            raise Exception("Failed to re-initialize session after reset")
        
        # Step 4: Update amazon client with new session
        logger.info("[Retry] Step 4: Updating Amazon client with fresh session...")
        self.amazon_client.update_session(new_session)
        
        logger.info("[Retry] Session reset complete")
    
    def _is_recoverable_error(self, error: Exception) -> bool:
        """
        Check if error is recoverable using structured classification.
        
        Recoverable errors:
        - SessionExpiredError (4xx authentication issues)
        - HTTPError with status codes in RECOVERABLE_STATUS_CODES
        - Network/connection errors
        """
        from services.amazon_client import HTTPError, SessionExpiredError
        
        # SessionExpiredError is always recoverable
        if isinstance(error, SessionExpiredError):
            return True
        
        # HTTPError with specific status codes
        if isinstance(error, HTTPError):
            if hasattr(error, 'status_code') and error.status_code in RECOVERABLE_STATUS_CODES:
                return True
        
        # Check for common network error patterns (fallback)
        error_str = str(error).lower()
        network_keywords = ['connection', 'timeout', 'reset', 'refused', 'unreachable']
        if any(keyword in error_str for keyword in network_keywords):
            return True
            
        return False
    
    def _record_error(self, amazon_return: 'AmazonReturn', error_msg: str):
        """
        Record error on the AmazonReturn object.
        
        Updates:
        - last_error: Error message
        """
        amazon_return.last_error = error_msg
        logger.info(f"[Retry] Recorded error for {amazon_return.return_request_id}")


def notify_user_of_failure(return_id: str, error: str):
    """
    Placeholder for user notification on failure.
    
    This function will be called when an API call fails even after
    session reset and retry.
    
    Args:
        return_id: The return request ID that failed
        error: Error message describing the failure
        
    TODO: Implement actual notification logic (email, webhook, etc.)
    """
    logger.warning(f"[NOTIFY] Return {return_id} failed: {error}")
    # TODO: Implement user notification
    # Examples:
    # - Send email notification
    # - Push to notification queue
    # - Write to alert file
    # - Call webhook endpoint
    pass
