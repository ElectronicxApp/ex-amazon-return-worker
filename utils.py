"""
Utility functions for Amazon return processing.

Provides common utilities:
- PDF to PNG conversion (for Amazon Alexandria uploads)
- CSRF token parsing
- UUID generation for S3 keys
"""

import re
import uuid
import logging
from typing import Optional
from io import BytesIO

import pymupdf  # PyMuPDF

logger = logging.getLogger(__name__)


def convert_pdf_to_png_bytes(pdf_input) -> bytes:
    """
    Convert PDF to PNG bytes for Amazon Alexandria upload.
    
    Amazon requires labels to be uploaded as PNG images.
    Uses PyMuPDF (fitz) for high-quality conversion at 300 DPI.
    
    Args:
        pdf_input: Either PDF file path (str) or PDF content as bytes
        
    Returns:
        PNG image as bytes
        
    Raises:
        ValueError: If PDF has no pages or conversion fails
    """
    try:
        # Open PDF from file path or bytes
        if isinstance(pdf_input, str):
            # File path provided
            doc = pymupdf.open(pdf_input)
        else:
            # Bytes provided
            doc = pymupdf.open(stream=pdf_input, filetype="pdf")
        
        if len(doc) == 0:
            raise ValueError("PDF has no pages")
        
        # Get first page
        page = doc[0]
        
        # Convert at 300 DPI for good quality
        zoom = 300 / 72  # 300 DPI
        mat = pymupdf.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)
        
        # Get PNG bytes
        png_bytes = pix.tobytes("png")
        
        doc.close()
        
        logger.info(f"[Utils] Converted PDF to PNG: {len(png_bytes)} bytes at 300 DPI")
        return png_bytes
        
    except Exception as e:
        logger.error(f"[Utils] Error converting PDF to PNG: {e}")
        raise


def parse_csrf(html_text: str) -> Optional[str]:
    """
    Parse CSRF token from Amazon Seller Central HTML response.
    
    Tries multiple patterns as Amazon may use different formats.
    
    Args:
        html_text: HTML response text
        
    Returns:
        CSRF token string or None if not found
    """
    patterns = [
        # Pattern from working amazon_bot: x-csrf-token' value='...'
        r"x-csrf-token['\"]?\s*value=['\"]([^'\"]+)['\"]",
        # Alternative: name="anti-csrftoken-a2z" value="..."
        r'name=["\']?anti-csrftoken-a2z["\']?\s+value=["\']([^"\']+)["\']',
        # JSON format in scripts
        r'anti-csrftoken-a2z["\s:]+([a-zA-Z0-9\-_]+)',
        r'"csrfToken"\s*:\s*"([^"]+)"',
    ]
    
    for pattern in patterns:
        try:
            match = re.search(pattern, html_text, re.IGNORECASE)
            if match:
                csrf = match.group(1)
                logger.info(f"[Utils] CSRF token found: {csrf[:10]}...")
                return csrf
        except Exception:
            continue
    
    logger.warning("[Utils] CSRF token not found in response")
    return None


def generate_s3_key(return_request_id: str) -> str:
    """
    Generate a unique S3 key for label upload.
    
    Format: {return_request_id}-{uuid}
    
    Args:
        return_request_id: Amazon return request ID
        
    Returns:
        Unique S3 key string
    """
    return f"{return_request_id}-{uuid.uuid1()}"
