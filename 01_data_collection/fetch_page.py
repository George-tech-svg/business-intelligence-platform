"""
FETCH PAGE MODULE - Handles all website requests
This module is responsible for communicating with the website.
It sends requests, receives responses, and handles failures with retry logic.

Author: Business Intelligence Platform
Date: 2026-05-12
"""

import requests
import time
import logging
from pathlib import Path
import sys

# Add project root to path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import SCRAPING_SETTINGS

# Setup logger for this module
logger = logging.getLogger(__name__)


def fetch_page(url, max_retries=3):
    """
    Fetch a webpage and return the HTML content.
    
    This function sends a request to the website and returns the HTML.
    If the request fails, it will retry up to 'max_retries' times.
    
    Parameters:
        url: The website address to fetch (example: "https://books.toscrape.com")
        max_retries: How many times to try if request fails (default is 3)
    
    Returns:
        HTML text as string if successful
        None if failed after all retries
    """
    
    # Try to fetch the page multiple times if needed
    for attempt in range(max_retries):
        try:
            # Log what we are doing
            logger.info(f"Fetching URL: {url} - Attempt {attempt + 1} of {max_retries}")
            
            # Pretend to be a real web browser
            # Many websites block requests that don't look like a browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            # Send the request to the website
            # timeout says how long to wait before giving up
            response = requests.get(
                url, 
                headers=headers, 
                timeout=SCRAPING_SETTINGS.get('timeout_seconds', 30)
            )
            
            # Check if request was successful
            # Status code 200 means "OK"
            # Status code 404 means "Page not found"
            # Status code 503 means "Server error"
            response.raise_for_status()
            
            # If we get here, request was successful
            logger.info(f"Successfully fetched: {url}")
            return response.text
            
        except requests.RequestException as error:
            # Log the error so we know what happened
            logger.warning(f"Attempt {attempt + 1} failed: {error}")
            
            # Wait before trying again
            # This is respectful to the website's server
            wait_time = SCRAPING_SETTINGS.get('rate_limit_seconds', 2)
            logger.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
    
    # If we get here, all attempts failed
    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
    return None


def fetch_multiple_urls(url_list, delay_between=1):
    """
    Fetch multiple URLs one after another.
    
    This is useful when you need to scrape many pages.
    The delay between requests prevents overloading the website.
    
    Parameters:
        url_list: List of website addresses to fetch
        delay_between: Seconds to wait between each request
    
    Returns:
        List of HTML texts (same order as URLs)
        Failed URLs return None in the list
    """
    
    results = []
    
    for index, url in enumerate(url_list):
        logger.info(f"Fetching URL {index + 1} of {len(url_list)}: {url}")
        
        # Fetch the page
        html = fetch_page(url)
        results.append(html)
        
        # Wait before next request (be polite to the server)
        if index < len(url_list) - 1:  # Don't wait after the last one
            logger.info(f"Waiting {delay_between} seconds before next request...")
            time.sleep(delay_between)
    
    return results


# This code runs only when you run this file directly
# It is for testing purposes
if __name__ == "__main__":
    print('-'*140)
    print("TESTING fetch_page.py MODULE")
    print('-'*140)
    
    # Setup basic logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Test with a known working website
    test_url = "https://books.toscrape.com"
    
    print(f"Testing fetch_page with: {test_url}")
    print('-'*140)
    
    result = fetch_page(test_url)
    
    if result:
        print(f"SUCCESS: Got HTML of length {len(result)} characters")
        print(f"First 200 characters: {result[:200]}...")
    else:
        print("FAILED: Could not fetch the page")
    
    print('-'*140)