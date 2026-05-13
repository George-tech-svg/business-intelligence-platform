"""
SCRAPE ALL MODULE - Orchestrates the complete scraping process
This module is the boss. It tells fetch_page, parse_books, and save_data
what to do and when to do it. It handles pagination and coordinates
the entire data collection workflow.

Author: Business Intelligence Platform
Date: 2026-05-12
"""

import time
import logging
from pathlib import Path
import sys

# Add project root to path so we can import config and other modules
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import our custom modules
from config import SCRAPING_SETTINGS, DATA_RAW
from fetch_page import fetch_page
from parse_books import parse_book_page
from save_data import save_books_to_csv, append_to_master_file

# Setup logger for this module
logger = logging.getLogger(__name__)


def scrape_single_page(url, page_number):
    """
    Scrape a single page of books.
    
    This function handles the complete workflow for one page:
    1. Fetch the HTML from the website
    2. Parse the HTML to extract book data
    3. Return the book data
    
    Parameters:
        url: The website address for the page
        page_number: The page number (for logging)
    
    Returns:
        List of books found on the page, or empty list if failed
    """
    
    logger.info(f"="*50)
    logger.info(f"Scraping page {page_number}: {url}")
    logger.info(f"="*50)
    
    # Step 1: Fetch the HTML from the website
    logger.info(f"Step 1: Fetching HTML from {url}")
    html = fetch_page(url)
    
    if html is None:
        logger.error(f"Failed to fetch page {page_number}. Skipping this page.")
        return []
    
    # Step 2: Parse the HTML to extract book data
    logger.info(f"Step 2: Parsing HTML to find books on page {page_number}")
    books_data = parse_book_page(html)
    
    if not books_data:
        logger.warning(f"No books found on page {page_number}")
    else:
        logger.info(f"Found {len(books_data)} books on page {page_number}")
    
    return books_data


def scrape_multiple_pages(start_url, max_pages=50):
    """
    Scrape multiple pages of books from the website.
    
    This function loops through pages 1, 2, 3, etc. until:
    - It reaches max_pages limit, OR
    - It finds a page with no books (end of catalog)
    
    Parameters:
        start_url: The URL of the first page (without page number)
        max_pages: Maximum number of pages to scrape (safety limit)
    
    Returns:
        List of all books from all pages
    """
    
    all_books = []
    current_page = 1
    
    logger.info("-"*140)
    logger.info("STARTING MULTI-PAGE SCRAPE")
    logger.info(f"Start URL: {start_url}")
    logger.info(f"Max pages limit: {max_pages}")
    logger.info("-"*140)
    
    while current_page <= max_pages:
        
        # Build the URL for the current page
        if current_page == 1:
            # Page 1 has no page number in the URL
            url = start_url
        else:
            # Pages 2, 3, 4 have page numbers
            url = f"{start_url}catalogue/page-{current_page}.html"
        
        # Scrape the current page
        books_on_page = scrape_single_page(url, current_page)
        
        # If no books found, assume we reached the end
        if not books_on_page:
            logger.info(f"No books found on page {current_page}. Stopping pagination.")
            break
        
        # Add books from this page to our collection
        all_books.extend(books_on_page)
        logger.info(f"Total books collected so far: {len(all_books)}")
        
        # Respectful delay between pages
        delay = SCRAPING_SETTINGS.get('rate_limit_seconds', 2)
        logger.info(f"Waiting {delay} seconds before next page...")
        time.sleep(delay)
        
        current_page += 1
    
    logger.info("-"*140)
    logger.info("MULTI-PAGE SCRAPE COMPLETE")
    logger.info(f"Pages scraped: {current_page - 1}")
    logger.info(f"Total books collected: {len(all_books)}")
    logger.info("-"*140)
    
    return all_books


def scrape_books_toscrape(max_pages=10):
    """
    Scrape all books from books.toscrape.com
    
    This is the main function for scraping the Books to Scrape website.
    It handles the complete workflow with proper error handling.
    
    Parameters:
        max_pages: Maximum number of pages to scrape (default 10)
                  Books to Scrape has about 50 pages total (1000 books)
                  Using 10 pages gives 200 books for testing
    
    Returns:
        Dictionary with results: {
            'success': True/False,
            'total_books': number,
            'file_path': path to saved CSV,
            'error': error message (if any)
        }
    """
    
    results = {
        'success': False,
        'total_books': 0,
        'file_path': None,
        'error': None
    }
    
    print('-'*140)
    print("BOOKS TO SCRAPE - PROFESSIONAL WEB SCRAPER")
    print('-'*140)
    print(f"Target website: https://books.toscrape.com")
    print(f"Max pages to scrape: {max_pages}")
    print(f"Expected books: ~{max_pages * 20} (20 books per page)")
    print('-'*140)
    
    try:
        # Step 1: Set up the starting URL
        base_url = "https://books.toscrape.com/"
        
        # Step 2: Scrape all pages
        logger.info("Starting scraping process...")
        all_books = scrape_multiple_pages(base_url, max_pages)
        
        if not all_books:
            results['error'] = "No books were scraped"
            logger.error(results['error'])
            return results
        
        results['total_books'] = len(all_books)
        logger.info(f"Successfully scraped {results['total_books']} books")
        
        # Step 3: Save the data to CSV
        logger.info("Saving scraped data to CSV...")
        file_path = save_books_to_csv(all_books)
        
        if file_path:
            results['file_path'] = str(file_path)
            logger.info(f"Data saved to: {file_path}")
        else:
            results['error'] = "Failed to save CSV file"
            logger.error(results['error'])
            return results
        
        # Step 4: Also append to master file (optional)
        try:
            append_to_master_file(all_books)
            logger.info("Also appended to master file for historical tracking")
        except Exception as e:
            logger.warning(f"Could not append to master file: {e}")
        
        results['success'] = True
        
        # Print summary
        print('-'*140)
        print("SCRAPING COMPLETED SUCCESSFULLY")
        print('-'*140)
        print(f"Total books scraped: {results['total_books']}")
        print(f"Data saved to: {results['file_path']}")
        print('-'*140)
        
        # Show first 5 books as preview
        print("\nPREVIEW OF SCRAPED DATA (First 5 books):")
        print('-'*140)
        for i, book in enumerate(all_books[:5], 1):
            print(f"{i}. {book['title']}")
            print(f"   Price: ${book['price']}")
            print(f"   Rating: {book['rating']} stars")
            print(f"   Available: {book['availability']}")
            print('-'*70)
        
    except Exception as error:
        results['error'] = str(error)
        logger.error(f"Scraping failed: {error}")
        print('-'*140)
        print(f"ERROR: Scraping failed - {error}")
        print('-'*140)
    
    return results


# This code runs only when you run this file directly
if __name__ == "__main__":
    
    # Setup logging to see what is happening
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run the scraper with max 3 pages for testing
    # Change to 50 to scrape all books
    result = scrape_books_toscrape(max_pages=3)
    
    if result['success']:
        print("\n" + "="*140)
        print("SCRAPING TEST PASSED")
        print(f"Total books collected: {result['total_books']}")
        print(f"Check your data folder: {result['file_path']}")
        print("="*140)
    else:
        print("\n" + "="*140)
        print("SCRAPING TEST FAILED")
        print(f"Error: {result['error']}")
        print("="*140)