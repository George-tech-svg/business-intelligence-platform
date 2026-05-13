"""
MASTER PIPELINE - Orchestrates the entire data pipeline
Run: python master_pipeline.py --now
"""

import subprocess
import sys
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import LOGS_DIR
from email_alerts import send_success_alert, send_failure_alert

print('-'*140)
print("MASTER PIPELINE - Complete Data Pipeline")
print('-'*140)

# Setup logging
LOGS_DIR.mkdir(parents=True, exist_ok=True)
log_filename = LOGS_DIR / f"master_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def run_script(script_path, description):
    """
    Run a Python script and return success status.
    """
    
    logger.info(f"Starting: {description}")
    logger.info(f"Script: {script_path}")
    
    try:
        # Add --auto flag to avoid user prompts
        result = subprocess.run(
            [sys.executable, str(script_path), '--auto'],
            capture_output=True,
            text=True,
            timeout=600
        )
        
        if result.returncode == 0:
            logger.info(f"SUCCESS: {description} completed")
            return True
        else:
            logger.error(f"FAILED: {description} failed with code {result.returncode}")
            logger.error(f"Error: {result.stderr[-500:]}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"FAILED: {description} timed out after 10 minutes")
        return False
    except Exception as error:
        logger.error(f"FAILED: {description} - {error}")
        return False


def run_full_pipeline():
    """
    Run the complete pipeline in order.
    """
    
    start_time = datetime.now()
    logger.info("="*70)
    logger.info("MASTER PIPELINE STARTED")
    logger.info(f"Start time: {start_time}")
    logger.info("="*70)
    
    results = {}
    
    # Step 1: Run scraper
    scraper_path = Path(__file__).parent.parent / "01_data_collection" / "run_scraper.py"
    results['scraper'] = run_script(scraper_path, "Web Scraper")
    
    if not results['scraper']:
        logger.error("Pipeline stopped: Scraper failed")
        send_failure_alert("Master Pipeline", "Scraper failed. Check logs for details.")
        return False
    
    # Step 2: Run ETL pipeline
    etl_path = Path(__file__).parent.parent / "02_data_engineering" / "run_etl.py"
    
    # Create responses for automated ETL
    responses = "1\n1\n1\nyes\n"
    
    try:
        result = subprocess.run(
            [sys.executable, str(etl_path)],
            input=responses,
            capture_output=True,
            text=True,
            timeout=600
        )
        results['etl'] = (result.returncode == 0)
        
        if results['etl']:
            logger.info("SUCCESS: ETL Pipeline completed")
        else:
            logger.error(f"ETL Pipeline failed: {result.stderr[-500:]}")
            
    except Exception as error:
        logger.error(f"ETL Pipeline error: {error}")
        results['etl'] = False
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("="*70)
    logger.info("MASTER PIPELINE COMPLETED")
    logger.info(f"End time: {end_time}")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info("="*70)
    logger.info("RESULTS:")
    for step, success in results.items():
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"  {step}: {status}")
    logger.info("="*70)
    
    # Send alerts
    if all(results.values()):
        send_success_alert("Master Pipeline", f"All steps completed successfully. Duration: {duration:.2f} seconds")
    else:
        send_failure_alert("Master Pipeline", f"Pipeline failed. Results: {results}")
    
    return all(results.values())


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        run_full_pipeline()
    else:
        print("Usage: python master_pipeline.py --now")
        print("This will run the complete pipeline once.")
        
        confirm = input("Run full pipeline now? (yes/no): ")
        if confirm.lower() in ['yes', 'y']:
            run_full_pipeline()
        else:
            print("Cancelled")