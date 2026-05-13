"""
RUN SCRAPER - Main entry point for the web scraper
How to run: 
  python run_scraper.py           - Interactive mode (asks for pages)
  python run_scraper.py --auto    - Automatic mode (uses default 3 pages)
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrape_all import scrape_books_toscrape
from config import LOGS_DIR

print('-'*140)
print("BUSINESS INTELLIGENCE PLATFORM - WEB SCRAPER")
print('-'*140)
print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print('-'*140)

# Setup logging
LOGS_DIR.mkdir(parents=True, exist_ok=True)
log_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f"scraper_run_{log_timestamp}.log"
log_filepath = LOGS_DIR / log_filename

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Check if running in automatic mode
auto_mode = '--auto' in sys.argv or '--now' in sys.argv

if auto_mode:
    print("\nRunning in AUTOMATIC mode (using default settings)")
    max_pages = 3
    print(f"Will scrape {max_pages} pages (approximately {max_pages * 20} books)")
    print('-'*140)
    proceed = 'yes'
else:
    # Ask user how many pages to scrape
    print("\nHow many pages do you want to scrape?")
    print("  - Each page contains 20 books")
    print("  - Recommended: 3 pages for testing (60 books)")
    print("  - Full scrape: 50 pages (1000 books)")
    print('-'*70)
    
    user_input = input("Enter number of pages (default 3): ").strip()
    
    if user_input == "":
        max_pages = 3
    else:
        max_pages = int(user_input)
    
    print('-'*140)
    print(f"Will scrape {max_pages} pages (approximately {max_pages * 20} books)")
    print('-'*140)
    
    proceed = input("Proceed with scraping? (yes/no): ").strip().lower()

if proceed not in ['yes', 'y']:
    print("Scraping cancelled by user.")
    logger.info("Scraping cancelled by user")
    sys.exit(0)

# Run the scraper
logger.info(f"Starting scrape with max_pages={max_pages}")
result = scrape_books_toscrape(max_pages=max_pages)

# Display results
print('-'*140)
print("SCRAPING RESULTS")
print('-'*140)

if result['success']:
    print("STATUS: SUCCESS")
    print(f"Total books scraped: {result['total_books']}")
    print(f"Data saved to: {result['file_path']}")
    print(f"Log file saved to: {log_filepath}")
else:
    print("STATUS: FAILED")
    print(f"Error: {result['error']}")

print('-'*140)
print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print('-'*140)