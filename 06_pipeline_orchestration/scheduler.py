"""
SCHEDULER - Automates the entire data pipeline
This script runs the scraper and ETL pipeline on a schedule.
It can be run manually or set up as a cron job / Task Scheduler.

Author: GEORGE ONYANGO COHIENG
Date: 2026-05-12
"""

import schedule
import time
import logging
import sys
from pathlib import Path
from datetime import datetime
import subprocess

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import LOGS_DIR

print('-'*140)
print("PHASE 6: AUTOMATION - SCHEDULER STARTING")
print('-'*140)

# Setup logging
LOGS_DIR.mkdir(parents=True, exist_ok=True)
log_filename = LOGS_DIR / f"scheduler_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def run_scraper():
    """
    Run the web scraper.
    """
    logger.info("="*70)
    logger.info("SCHEDULED TASK: Starting web scraper")
    logger.info("="*70)
    
    try:
        scraper_path = Path(__file__).parent.parent / "01_data_collection" / "run_scraper.py"
        
        result = subprocess.run(
            [sys.executable, str(scraper_path)],
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        
        if result.returncode == 0:
            logger.info("SUCCESS: Scraper completed successfully")
            logger.info(f"Output: {result.stdout[-500:]}")  # Last 500 chars
        else:
            logger.error(f"FAILED: Scraper failed with code {result.returncode}")
            logger.error(f"Error: {result.stderr[-500:]}")
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        logger.error("FAILED: Scraper timed out after 10 minutes")
        return False
    except Exception as error:
        logger.error(f"FAILED: Unexpected error - {error}")
        return False


def run_etl():
    """
    Run the ETL pipeline.
    """
    logger.info("="*70)
    logger.info("SCHEDULED TASK: Starting ETL pipeline")
    logger.info("="*70)
    
    try:
        etl_path = Path(__file__).parent.parent / "02_data_engineering" / "run_etl.py"
        
        # Create a response file to auto-answer prompts
        # This simulates user input for automated runs
        responses = "1\n1\n1\nyes\n"
        
        result = subprocess.run(
            [sys.executable, str(etl_path)],
            input=responses,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        
        if result.returncode == 0:
            logger.info("SUCCESS: ETL pipeline completed successfully")
            logger.info(f"Output: {result.stdout[-500:]}")
        else:
            logger.error(f"FAILED: ETL pipeline failed with code {result.returncode}")
            logger.error(f"Error: {result.stderr[-500:]}")
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        logger.error("FAILED: ETL pipeline timed out after 10 minutes")
        return False
    except Exception as error:
        logger.error(f"FAILED: Unexpected error - {error}")
        return False


def run_full_pipeline():
    """
    Run the complete pipeline: scraper then ETL.
    """
    logger.info("="*70)
    logger.info("SCHEDULED TASK: Starting FULL PIPELINE")
    logger.info("="*70)
    
    start_time = datetime.now()
    logger.info(f"Pipeline started at: {start_time}")
    
    # Step 1: Run scraper
    scraper_success = run_scraper()
    
    if not scraper_success:
        logger.error("FULL PIPELINE FAILED: Scraper failed")
        return False
    
    # Step 2: Run ETL
    etl_success = run_etl()
    
    if not etl_success:
        logger.error("FULL PIPELINE FAILED: ETL failed")
        return False
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("="*70)
    logger.info(f"FULL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info(f"End time: {end_time}")
    logger.info("="*70)
    
    return True


def setup_schedule():
    """
    Set up the automated schedule.
    """
    # Schedule the full pipeline to run daily at 2 AM
    schedule.every().day.at("02:00").do(run_full_pipeline)
    
    # Optional: Schedule scraper and ETL separately
    # schedule.every().day.at("02:00").do(run_scraper)
    # schedule.every().day.at("03:00").do(run_etl)
    
    logger.info("="*70)
    logger.info("SCHEDULE CONFIGURATION")
    logger.info("="*70)
    logger.info("Full pipeline scheduled for: 02:00 AM daily")
    logger.info("")
    logger.info("To run immediately (for testing): python scheduler.py --now")
    logger.info("To run once and exit: python scheduler.py --once")
    logger.info("To run continuously: python scheduler.py")
    logger.info("="*70)


def run_once():
    """
    Run the pipeline once immediately (for testing).
    """
    logger.info("Running pipeline once (manual trigger)")
    run_full_pipeline()


def run_continuously():
    """
    Run the scheduler continuously.
    """
    setup_schedule()
    
    logger.info("")
    logger.info("SCHEDULER IS RUNNING")
    logger.info("Press Ctrl+C to stop")
    logger.info("")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("")
        logger.info("Scheduler stopped by user")
        logger.info("Goodbye!")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--now":
            run_once()
        elif sys.argv[1] == "--once":
            run_once()
        elif sys.argv[1] == "--scraper":
            run_scraper()
        elif sys.argv[1] == "--etl":
            run_etl()
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            print("Usage:")
            print("  python scheduler.py           - Run continuously")
            print("  python scheduler.py --now     - Run pipeline once")
            print("  python scheduler.py --scraper - Run scraper only")
            print("  python scheduler.py --etl     - Run ETL only")
    else:
        run_continuously()