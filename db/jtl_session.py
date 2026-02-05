"""
JTL Session - Local connection to JTL SQL Server.

Direct access to JTL Wawi database for RMA lookups and order details.
"""
import logging
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

import pyodbc

from config import settings

logger = logging.getLogger(__name__)


class JTLSession:
    """
    JTL SQL Server connection manager.
    
    Connects directly to JTL Wawi database running on the same server.
    """
    
    def __init__(self):
        self.conn_str = (
            "DRIVER={SQL Server};"
            f"SERVER={settings.JTL_SQL_SERVER};"
            f"DATABASE={settings.JTL_SQL_DATABASE};"
            f"UID={settings.JTL_SQL_USERNAME};"
            f"PWD={settings.JTL_SQL_PASSWORD}"
        )
    
    @contextmanager
    def get_connection(self) -> pyodbc.Connection:
        """
        Context manager for database connections.
        
        Usage:
            with jtl.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(...)
        """
        conn = None
        try:
            conn = pyodbc.connect(self.conn_str, timeout=10)
            yield conn
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Execute a query and return results as list of dicts.
        
        Args:
            query: SQL query string with ? placeholders
            params: Tuple of parameter values
            
        Returns:
            List of dicts with column names as keys
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
        except pyodbc.Error as e:
            logger.error(f"JTL query error: {e}")
            return []
    
    def test_connection(self) -> bool:
        """Test JTL SQL Server connection."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
            logger.info(f"JTL SQL Server connection successful: {settings.JTL_SQL_SERVER}")
            return True
        except Exception as e:
            logger.error(f"JTL SQL Server connection failed: {e}")
            return False


# Global instance
jtl_session = JTLSession()
