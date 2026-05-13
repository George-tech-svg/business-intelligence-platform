"""
SAVE DATA MODULE - Saves scraped data to CSV files
This module takes the cleaned book data and saves it to a CSV file.
CSV files can be opened in Excel, Google Sheets, or loaded into a database.

Author: Business Intelligence Platform
Date: 2026-05-12
"""

import csv
import logging
from pathlib import Path
from datetime import datetime
import sys

# Add project root to path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_RAW

# Setup logger for this module
logger = logging.getLogger(__name__)


def save_books_to_csv(books_data, filename=None):
    """
    Save list of books to a CSV file.
    
    This function takes the list of books (each book is a dictionary)
    and writes them to a CSV file. The CSV file has headers and one row per book.
    
    Parameters:
        books_data: List of dictionaries. Each dictionary has keys:
                    'title', 'price', 'rating', 'availability', 'scraped_at'
        filename: Name of the CSV file (optional)
                  If not provided, creates filename with current timestamp
    
    Returns:
        Path object pointing to the saved file
        None if save failed
    """
    
    # Check if we have any data to save
    if not books_data:
        logger.warning("No book data to save. List is empty.")
        return None
    
    logger.info(f"Preparing to save {len(books_data)} books to CSV")
    
    # Create filename with timestamp if not provided
    # Example output: books_20260512_143025.csv
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"books_{timestamp}.csv"
    
    # Make sure the data/raw directory exists
    # parents=True means create parent folders if they don't exist
    # exist_ok=True means don't error if folder already exists
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    
    # Create the full file path
    # Example: C:/.../business_intelligence_platform/data/raw/books_20260512_143025.csv
    file_path = DATA_RAW / filename
    
    # Define the column headers in the order we want them in the CSV
    # This order makes the CSV easy to read for humans
    fieldnames = ['title', 'price', 'rating', 'availability', 'scraped_at']
    
    try:
        # Open the file for writing
        # newline='' prevents blank lines between rows in the CSV
        # encoding='utf-8' handles special characters like £ or é
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            
            # Create a CSV writer object
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write the header row (column names)
            writer.writeheader()
            
            # Write all the book data rows
            # Each book in books_data is a dictionary
            writer.writerows(books_data)
        
        logger.info(f"SUCCESS: Saved {len(books_data)} books to {file_path}")
        print(f"  Data saved to: {file_path}")
        
        return file_path
        
    except Exception as error:
        logger.error(f"Failed to save CSV file: {error}")
        return None


def save_books_to_multiple_formats(books_data, base_filename=None):
    """
    Save books data to multiple formats (CSV, JSON) for flexibility.
    
    Sometimes you need data in different formats for different tools.
    CSV is good for Excel. JSON is good for web applications.
    
    Parameters:
        books_data: List of dictionaries with book data
        base_filename: Base name without extension (optional)
    
    Returns:
        Dictionary with paths to each saved file
    """
    
    saved_files = {}
    
    # Create base filename with timestamp if not provided
    if base_filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"books_{timestamp}"
    
    # Save as CSV
    csv_path = save_books_to_csv(books_data, f"{base_filename}.csv")
    if csv_path:
        saved_files['csv'] = csv_path
    
    # Save as JSON (optional - useful for APIs)
    try:
        import json
        json_path = DATA_RAW / f"{base_filename}.json"
        
        with open(json_path, 'w', encoding='utf-8') as jsonfile:
            json.dump(books_data, jsonfile, indent=2, ensure_ascii=False)
        
        saved_files['json'] = json_path
        logger.info(f"SUCCESS: Saved {len(books_data)} books to {json_path}")
        
    except Exception as error:
        logger.warning(f"Could not save JSON file: {error}")
    
    return saved_files


def append_to_master_file(books_data, master_filename="all_books_master.csv"):
    """
    Append new books to a master file that keeps all scraped books.
    
    This is useful when you run the scraper multiple times.
    Each run adds new books to the master file instead of creating separate files.
    
    Parameters:
        books_data: List of dictionaries with book data
        master_filename: Name of the master CSV file
    
    Returns:
        True if successful, False if failed
    """
    
    # Make sure the data/raw directory exists
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    
    # Full path to the master file
    master_path = DATA_RAW / master_filename
    
    fieldnames = ['title', 'price', 'rating', 'availability', 'scraped_at']
    
    try:
        # Check if file already exists
        file_exists = master_path.exists()
        
        # Open file in append mode ('a' means add to end of file)
        with open(master_path, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header only if file is new
            if not file_exists:
                writer.writeheader()
                logger.info(f"Created new master file: {master_path}")
            
            # Append the new books
            writer.writerows(books_data)
        
        logger.info(f"Appended {len(books_data)} books to master file")
        return True
        
    except Exception as error:
        logger.error(f"Failed to append to master file: {error}")
        return False


def get_latest_csv_file():
    """
    Find the most recently saved CSV file in the data/raw folder.
    
    This is useful when other scripts need to read the latest scraped data.
    
    Returns:
        Path to the latest CSV file, or None if no files found
    """
    
    # Get all CSV files in the data/raw folder
    csv_files = list(DATA_RAW.glob("books_*.csv"))
    
    if not csv_files:
        logger.warning("No CSV files found in data/raw folder")
        return None
    
    # Find the most recent file by modification time
    latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)
    
    logger.info(f"Latest CSV file: {latest_file}")
    return latest_file


# This code runs only when you run this file directly
# It is for testing purposes
if __name__ == "__main__":
    print('-'*140)
    print("TESTING save_data.py MODULE")
    print('-'*140)
    
    # Setup basic logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Sample book data for testing
    sample_books = [
        {
            'title': 'Test Book 1',
            'price': '51.77',
            'rating': 'Three',
            'availability': 'In stock',
            'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        {
            'title': 'Test Book 2',
            'price': '53.74',
            'rating': 'One',
            'availability': 'In stock',
            'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    ]
    
    print(f"Testing save_books_to_csv with {len(sample_books)} sample books")
    print('-'*140)
    
    # Test saving to CSV
    saved_path = save_books_to_csv(sample_books)
    
    if saved_path:
        print(f"SUCCESS: Test file saved to {saved_path}")
        
        # Verify file was created and has content
        if saved_path.exists():
            file_size = saved_path.stat().st_size
            print(f"File size: {file_size} bytes")
            
            # Read back the file to verify
            import csv
            with open(saved_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
                print(f"Number of rows in file: {len(rows)}")
                print(f"Header row: {rows[0]}")
                print(f"First data row: {rows[1]}")
    else:
        print("FAILED: Could not save test file")
    
    print('-'*140)
    print("TESTING COMPLETE")
    print('-'*140)