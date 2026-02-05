"""
Session Manager - Browser-based Amazon authentication.

Handles login, session maintenance, and cookie persistence.
Key features:
- Fresh login for each processing cycle (via hard_reset + ensure_session)
- Nodriver-based browser automation
- Cookie extraction for API calls
- Robust error handling with exponential backoff
- Proper browser process cleanup
- Browser profile stored in ./Browser/profile
- Cookies stored in ./Browser/amazonCookies.pkl
"""

import asyncio
import logging
import os
import pickle
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
import pyotp

from config import settings

logger = logging.getLogger(__name__)

# Try to import nodriver
try:
    import nodriver
except ImportError:
    nodriver = None
    logger.warning("nodriver not installed, browser automation unavailable")


class SessionExpiredError(Exception):
    """Raised when Amazon session has expired."""
    pass


class SessionManager:
    """
    Manages Amazon Seller Central session via browser automation.
    
    Lifecycle (fresh login each cycle):
    1. Call hard_reset() at start of each worker cycle
    2. Call init_session_for_cycle() to perform fresh login
    3. Use get_session() to get requests.Session for API calls
    4. Call stop_browser() at end of cycle
    
    Browser data is stored in:
    - Profile: ./Browser/profile
    - Cookies: ./Browser/amazonCookies.pkl
    """
    
    _instance: Optional['SessionManager'] = None
    _lock = threading.Lock()
    
    # Maximum total time for login attempts (10 minutes)
    MAX_LOGIN_TIME_SECONDS = 600
    
    def __new__(cls, *args, **kwargs):
        """Singleton pattern for thread-safe session access."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize session manager with paths from config."""
        if self._initialized:
            return
        
        # Use paths from config (./Browser folder)
        self.browser_dir = settings.BROWSER_DIR
        self.data_dir = settings.BROWSER_PROFILE_DIR
        self.cookie_path = settings.AMAZON_COOKIE_FILE
        
        self.browser: Optional['nodriver.Browser'] = None
        self._session: Optional[requests.Session] = None
        self._session_valid = False
        self._last_refresh: Optional[datetime] = None
        self._refresh_lock = asyncio.Lock()
        self._operation_lock = asyncio.Lock()
        
        # Credentials
        self.email = settings.AMAZON_EMAIL
        self.password = settings.AMAZON_PASSWORD
        self.otp_secret = settings.AMAZON_2FA_SECRET
        
        # Ensure Browser directories exist
        Path(self.browser_dir).mkdir(parents=True, exist_ok=True)
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        
        self._initialized = True
        logger.info(f"SessionManager initialized:")
        logger.info(f"  Browser dir: {self.browser_dir}")
        logger.info(f"  Profile dir: {self.data_dir}")
        logger.info(f"  Cookie file: {self.cookie_path}")
        
    async def init_browser(self, headless: bool = True) -> 'nodriver.Browser':
        """Initialize the nodriver browser instance."""
        if nodriver is None:
            raise RuntimeError("nodriver not installed - run: pip install nodriver")
            
        try:
            logger.info(f"Initializing browser with data_dir: {self.data_dir}")
            self.browser = await nodriver.start(
                headless=headless,
                sandbox=False,
                user_data_dir=self.data_dir,
                lang="en-US",
                browser_args=['--disable-gpu', '--no-sandbox', '--disable-dev-shm-usage']
            )
            return self.browser
        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            raise

    async def is_logged_in(self) -> bool:
        """
        Check if the session is still active on Seller Central.
        
        Returns:
            False if any check fails (triggers hard reset)
        """
        if not self.browser:
            logger.warning("Browser not initialized.")
            return False
       
        url = 'https://sellercentral.amazon.de/gp/returns/list/v2'
        
        try:
            page = await self.browser.get(url)
            await asyncio.sleep(3)
            
            # Check for sign-in form
            signin_form = await page.select('form[name="signIn"]', timeout=10)
            otp_form = await page.select('input[id="auth-mfa-otpcode"]', timeout=10)
            country_select = await page.select('button.full-page-account-switcher-account-details', timeout=5)

            
            if signin_form or otp_form or country_select:
                logger.warning("Browser is NOT logged in - login form detected.")
                return False
            
            logger.info("Login form not found - session appears valid.")
            return True
                
        except:
            logger.warning("NOT logged in - will perform hard reset.")
            return False

    async def login(self) -> bool:
        """Login to Amazon Seller Central."""
        if not self.browser:
            raise RuntimeError("Browser not initialized")
            
        page = self.browser.tabs[0]
        await asyncio.sleep(10)
        
        # Step 1: Handle email input
        try:
            email_box = await page.select('input[name="email"]', timeout=10)
            if email_box:
                js = '''document.querySelector('input[name="email"]').hidden'''
                is_email_box_hidden = await page.evaluate(js, return_by_value=True)
                logger.info(f"Email box hidden status: {is_email_box_hidden.value}")
                
                if not is_email_box_hidden.value:
                    await email_box.clear_input()
                    await asyncio.sleep(5)
                    await email_box.send_keys(self.email)
                    await asyncio.sleep(5)
                    
                    continue_button = await page.select('input[id="continue"]', timeout=10)
                    if continue_button:
                        logger.info("Continue button found, clicking...")
                        await continue_button.click()
                        await asyncio.sleep(7)
                    else:
                        logger.error("Continue button not found.")
                else:
                    logger.info("Email box is hidden, skipping email step.")
            else:
                logger.error("Email box not found.")
        except Exception as e:
            logger.error(f"Failed to find email box: {e}")
        
        # Step 2: Handle password input
        logger.info("Looking for password box...")
        password_box = await page.select('input[name="password"]', timeout=10)
        if password_box:
            logger.info("Password box found.")
            await password_box.clear_input()
            await asyncio.sleep(5)
            await password_box.send_keys(self.password)
            await asyncio.sleep(5)
        else:
            logger.error("Password box not found.")

        try:
            # Step 3: Check remember me checkbox
            remember_me = await page.select('input[name="rememberMe"]', timeout=10)
            if remember_me:
                logger.info("Remember me checkbox found, clicking...")
                await remember_me.click()
                await asyncio.sleep(5)
            
            # Step 4: Click sign in button
            sign_in_button = await page.select('input[id="signInSubmit"]', timeout=10)
            if sign_in_button:
                logger.info("Sign in button found, clicking...")
                await sign_in_button.click()
                await asyncio.sleep(10)
            
            # Step 5: Handle OTP if required
            logger.info("Checking for OTP box...")
            otp_box = await page.select('input[id="auth-mfa-otpcode"]', timeout=15)
            if otp_box:
                logger.info("OTP box found, generating OTP...")
                await asyncio.sleep(5)
                await otp_box.clear_input()
                await asyncio.sleep(5)
                
                totp = pyotp.TOTP(self.otp_secret)
                otp_code = totp.now()
                logger.info(f"Generated OTP code: {otp_code}")
                await otp_box.send_keys(otp_code)
                await asyncio.sleep(5)

                remember_device = await page.select('input[id="auth-mfa-remember-device"]', timeout=10)
                if remember_device:
                    logger.info("Remember device checkbox found, clicking...")
                    await remember_device.click()
                    await asyncio.sleep(5)
                
                otp_signin_button = await page.select('input[id="auth-signin-button"]', timeout=10)
                if otp_signin_button:
                    logger.info("OTP sign in button found, clicking...")
                    await asyncio.sleep(5)
                    await otp_signin_button.click()
                    await asyncio.sleep(10)
            else:
                logger.info("OTP box not found, may not be required.")
            
            # Step 6: Select an account if prompted
            logger.info("Checking for account selection...")
            if 'account-switcher' in page.url:
                js_click = '''const all_elements=document.getElementsByClassName('full-page-account-switcher-account-details'); 
                function country(){
                for(let i=0;i<all_elements.length;i++){ if(all_elements[i].innerText.includes('Germany')){ all_elements[i].click(); return true; } } return false;
                 } 
                 country();'''
                result = await page.evaluate(js_click)
                if not result:
                    logger.error("Failed to select account for Germany.")
                    return False
                    
                await asyncio.sleep(10)
                submit_btn_click_js='''document.getElementsByClassName('full-page-account-switcher-button')[0].click();'''
                await page.evaluate(submit_btn_click_js)
                await asyncio.sleep(10)
            else:
                logger.info("Account selection not required.")
                    
            logger.info("Login process completed.")
            return True
            
        except Exception as e:
            logger.error(f"Failed during login process: {e}")
            return False

    async def refresh_cookies(self) -> Optional[requests.Session]:
        """Extract cookies from browser and create requests session."""
        if not self.browser:
            logger.error("Browser not initialized. Cannot refresh cookies.")
            return None
        
        try:
            logger.info("Extracting cookies from browser...")
            requests_style_cookies = await self.browser.cookies.get_all(
                requests_cookie_format=True
            )
            
            session = requests.Session()
            for cookie in requests_style_cookies:
                session.cookies.set_cookie(cookie)
            
            # Save to pickle file in Browser folder
            with open(self.cookie_path, 'wb') as f:
                pickle.dump(session, f)
            
            self._session = session
            self._session_valid = True
            self._last_refresh = datetime.utcnow()
            
            logger.info(f"Cookies saved to {self.cookie_path}")
            return session
            
        except Exception as e:
            logger.error(f"Failed to refresh cookies: {e}")
            return None

    async def stop_browser(self):
        """Stop the browser process with thorough cleanup."""
        if not self.browser:
            return
            
        logger.info("Stopping browser process...")
        
        try:
            # Cancel browser-related tasks
            try:
                all_tasks = asyncio.all_tasks()
                browser_tasks = []
                
                for task in all_tasks:
                    coro = task.get_coro()
                    coro_name = coro.__qualname__ if coro else ""
                    
                    if any(pattern in str(coro_name) for pattern in [
                        'Browser.update_targets',
                        'Browser._get_targets',
                        'connection.send',
                        'Tab.',
                        'browser.'
                    ]):
                        browser_tasks.append(task)
                
                for task in browser_tasks:
                    if not task.done():
                        task.cancel()
                
                if browser_tasks:
                    await asyncio.sleep(0.5)
                    logger.info(f"Cancelled {len(browser_tasks)} browser background tasks")
                    
            except Exception as e:
                logger.debug(f"Error cancelling browser tasks: {e}")
            
            # Close all tabs
            for tab in list(self.browser.tabs):
                try:
                    await tab.close()
                except Exception:
                    pass
            
            # Close websocket connection
            try:
                if hasattr(self.browser, 'connection') and self.browser.connection:
                    if hasattr(self.browser.connection, '_websocket') and self.browser.connection._websocket:
                        try:
                            await self.browser.connection._websocket.close()
                        except Exception:
                            pass
            except Exception:
                pass
            
            # Stop the browser process
            try:
                self.browser.stop()
            except Exception:
                pass
                    
        except Exception as e:
            logger.warning(f"Error while stopping browser: {e}")
        finally:
            self.browser = None
            await asyncio.sleep(2)
        
        logger.info("Browser stopped.")

    async def ensure_session(self, max_attempts: int = 10) -> Optional[requests.Session]:
        """
        Ensure a valid session exists with exponential backoff and time limit.
        
        Args:
            max_attempts: Maximum login attempts (default: 10)
        
        Returns:
            requests.Session if successful, None if all attempts fail
        """
        async with self._operation_lock:
            attempt = 0
            start_time = time.time()
            base_wait = 5
            backoff_multiplier = 1.5
            
            while attempt < max_attempts:
                elapsed = time.time() - start_time
                if elapsed >= self.MAX_LOGIN_TIME_SECONDS:
                    logger.error(f"Login timeout: exceeded {self.MAX_LOGIN_TIME_SECONDS}s limit")
                    return None
                
                attempt += 1
                logger.info(f"=== Login attempt {attempt}/{max_attempts} (elapsed: {elapsed:.0f}s) ===")
                
                try:
                    if not self.browser:
                        await self.init_browser()
                    
                    if await self.is_logged_in():
                        logger.info("Already logged in!")
                        return await self.refresh_cookies()
                    
                    logger.info("Not logged in, performing login...")
                    login_success = await self.login()
                    
                    if login_success:
                        logger.info("Login completed, verifying with API...")
                        
                        if await self.is_logged_in():
                            logger.info(f"Login verified on attempt {attempt}!")
                            return await self.refresh_cookies()
                        else:
                            logger.warning("Login verification failed.")
                    else:
                        logger.warning("Login returned False.")
                    
                    logger.error(f"Login attempt {attempt} failed, performing hard reset...")
                    await self.hard_reset()
                    
                    wait_time = min(60, base_wait * (backoff_multiplier ** (attempt - 1)))
                    logger.info(f"Waiting {wait_time:.0f}s before retry (exponential backoff)...")
                    await asyncio.sleep(wait_time)
                    
                except Exception as e:
                    logger.error(f"Error during login attempt {attempt}: {e}")
                    await self.hard_reset()
                    
                    wait_time = min(60, base_wait * (backoff_multiplier ** (attempt - 1)))
                    await asyncio.sleep(wait_time)
            
            logger.error(f"All {max_attempts} login attempts failed!")
            return None

    async def init_session_for_cycle(self) -> requests.Session:
        """
        Initialize/refresh session at the start of a worker cycle.
        
        Raises:
            SessionExpiredError: If unable to establish valid session
        """
        async with self._refresh_lock:
            logger.info("=== Initializing session for new cycle ===")
            
            try:
                session = await self.ensure_session()
                
                if session is None:
                    raise SessionExpiredError("Failed to establish Amazon session")
                
                logger.info(f"Session initialized successfully at {self._last_refresh}")
                return session
                
            except Exception as e:
                logger.error(f"Failed to init session for cycle: {e}")
                raise SessionExpiredError(f"Session initialization failed: {e}")

    async def hard_reset(self) -> None:
        """
        Perform a complete hard reset of the session.
        
        Steps:
        1. Stop browser with thorough cleanup
        2. Delete pickle cookie file
        3. Clear all session state
        """
        logger.warning("=== HARD RESET: Clearing all session data ===")
        
        await self.stop_browser()
        
        if os.path.exists(self.cookie_path):
            try:
                os.remove(self.cookie_path)
                logger.info(f"Deleted cookie file: {self.cookie_path}")
            except Exception as e:
                logger.error(f"Failed to delete cookie file: {e}")
        
        self._session = None
        self._session_valid = False
        self._last_refresh = None
        
        logger.info("Hard reset complete - session fully cleared")

    def get_session(self) -> Optional[requests.Session]:
        """
        Get the current requests session (synchronous).
        
        Returns the in-memory session or loads from pickle if available.
        """
        if self._session and self._session_valid:
            return self._session
            
        try:
            if os.path.exists(self.cookie_path):
                with open(self.cookie_path, 'rb') as f:
                    self._session = pickle.load(f)
                    self._session_valid = True
                    logger.info("Session loaded from cookie file")
                    return self._session
        except Exception as e:
            logger.error(f"Failed to load session from pickle: {e}")
            
        return None

    def invalidate_session(self):
        """Mark current session as invalid (after API error)."""
        self._session_valid = False
        logger.warning("Session marked as invalid")

    @property
    def is_valid(self) -> bool:
        """Check if current session is valid."""
        return self._session_valid and self._session is not None

    @property
    def last_refresh_time(self) -> Optional[datetime]:
        """Get timestamp of last session refresh."""
        return self._last_refresh

    def get_status(self) -> dict:
        """Get session manager status for health checks."""
        return {
            "session_valid": self._session_valid,
            "browser_active": self.browser is not None,
            "last_refresh": self._last_refresh.isoformat() if self._last_refresh else None,
            "cookie_file_exists": os.path.exists(self.cookie_path),
            "browser_dir": self.browser_dir,
            "profile_dir": self.data_dir,
            "max_login_time": self.MAX_LOGIN_TIME_SECONDS
        }


# Global singleton instance
session_manager = SessionManager()
