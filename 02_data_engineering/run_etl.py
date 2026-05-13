"""
RUN ETL PIPELINE - Main orchestrator for the ETL process
This file runs the complete ETL pipeline from start to finish.
It calls all the other modules in the correct order.
This is the only file you need to run for the ETL process.

How to run: python run_etl.py

Author: Business Intelligence Platform
Date: 2026-05-12
"""
from update_outputs import update_outputs
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import ETL modules
from read_data import read_csv_to_dataframe, save_processed_data, get_data_info
from clean_data import run_full_cleaning
from transform_data import run_full_transformation
from validate_data import run_full_validation, should_proceed_to_load
from load_to_mysql import run_full_load

# Import config for paths
from config import LOGS_DIR

print('-'*140)
print("BUSINESS INTELLIGENCE PLATFORM - ETL PIPELINE")
print('-'*140)
print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print('-'*140)

# -----------------------------------------------------------------------------
# SETUP LOGGING
# -----------------------------------------------------------------------------

# Create logs directory if it does not exist
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Create a log filename with timestamp
log_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f"etl_run_{log_timestamp}.log"
log_filepath = LOGS_DIR / log_filename

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

logger.info("="*140)
logger.info("ETL PIPELINE STARTED")
logger.info(f"Log file: {log_filepath}")
logger.info("="*140)

# -----------------------------------------------------------------------------
# MAIN ETL FUNCTION
# -----------------------------------------------------------------------------

def run_etl_pipeline(csv_file_path=None, skip_validation=False, skip_duplicates=True):
    """
    Run the complete ETL pipeline.
    
    Parameters:
        csv_file_path: Path to CSV file (optional, uses latest if None)
        skip_validation: If True, skip validation and always load
        skip_duplicates: If True, skip books already in database
    
    Returns:
        Dictionary with pipeline results
    """
    
    results = {
        'success': False,
        'rows_read': 0,
        'rows_after_cleaning': 0,
        'rows_after_transformation': 0,
        'rows_loaded': 0,
        'validation_passed': False,
        'errors': [],
        'steps_completed': []
    }
    
    print('-'*140)
    print("STARTING ETL PIPELINE")
    print('-'*140)
    
    # -------------------------------------------------------------------------
    # STEP 1: READ DATA
    # -------------------------------------------------------------------------
    
    print("\n[STEP 1 OF 5] READING DATA FROM CSV")
    print('-'*70)
    
    try:
        df = read_csv_to_dataframe(csv_file_path)
        
        if df is None or df.empty:
            results['errors'].append("No data read from CSV file")
            results['success'] = False
            print("ERROR: No data to process")
            return results
        
        results['rows_read'] = len(df)
        results['steps_completed'].append('read_data')
        print(f"SUCCESS: Read {results['rows_read']} rows")
        
    except Exception as error:
        results['errors'].append(f"Read failed: {error}")
        logger.error(f"Read step failed: {error}")
        return results
    
    # -------------------------------------------------------------------------
    # STEP 2: CLEAN DATA
    # -------------------------------------------------------------------------
    
    print("\n[STEP 2 OF 5] CLEANING DATA")
    print('-'*70)
    
    try:
        df = run_full_cleaning(df)
        
        if df is None or df.empty:
            results['errors'].append("No data after cleaning")
            results['success'] = False
            print("ERROR: No data remaining after cleaning")
            return results
        
        results['rows_after_cleaning'] = len(df)
        results['steps_completed'].append('clean_data')
        
        rows_removed = results['rows_read'] - results['rows_after_cleaning']
        print(f"SUCCESS: Cleaned data. Removed {rows_removed} rows")
        
    except Exception as error:
        results['errors'].append(f"Cleaning failed: {error}")
        logger.error(f"Cleaning step failed: {error}")
        return results
    
    # -------------------------------------------------------------------------
    # STEP 3: TRANSFORM DATA
    # -------------------------------------------------------------------------
    
    print("\n[STEP 3 OF 5] TRANSFORMING DATA")
    print('-'*70)
    
    try:
        df = run_full_transformation(df)
        
        if df is None or df.empty:
            results['errors'].append("No data after transformation")
            results['success'] = False
            print("ERROR: No data after transformation")
            return results
        
        results['rows_after_transformation'] = len(df)
        results['steps_completed'].append('transform_data')
        print(f"SUCCESS: Transformed data. Added calculated columns")
        
        # Save processed data as backup
        save_processed_data(df)
        
    except Exception as error:
        results['errors'].append(f"Transformation failed: {error}")
        logger.error(f"Transformation step failed: {error}")
        return results
    
    # -------------------------------------------------------------------------
    # STEP 4: VALIDATE DATA
    # -------------------------------------------------------------------------
    
    print("\n[STEP 4 OF 5] VALIDATING DATA QUALITY")
    print('-'*70)
    
    try:
        validation_result = run_full_validation(df)
        results['validation_passed'] = validation_result['overall_pass']
        
        if skip_validation:
            print("NOTE: Validation skipped by user request")
            results['validation_passed'] = True
        elif not validation_result['overall_pass']:
            results['errors'].append("Data validation failed")
            print("ERROR: Data quality checks failed")
            print("Please fix data quality issues before loading")
            return results
        
        results['steps_completed'].append('validate_data')
        print(f"SUCCESS: Validation passed. Quality score: {validation_result['quality_score']:.1f}%")
        
    except Exception as error:
        results['errors'].append(f"Validation failed: {error}")
        logger.error(f"Validation step failed: {error}")
        return results
    
    # -------------------------------------------------------------------------
    # STEP 5: LOAD TO MYSQL
    # -------------------------------------------------------------------------
    
    print("\n[STEP 5 OF 5] LOADING DATA TO MYSQL")
    print('-'*70)
    
    try:
        load_result = run_full_load(df, skip_duplicates=skip_duplicates)
        
        if load_result['success']:
            results['rows_loaded'] = load_result['rows_inserted']
            results['success'] = True
            results['steps_completed'].append('load_to_mysql')
            print(f"SUCCESS: Loaded {results['rows_loaded']} rows to database")
        else:
            results['errors'].extend(load_result['errors'])
            print(f"ERROR: Load failed - {load_result['message']}")
        
    except Exception as error:
        results['errors'].append(f"Load failed: {error}")
        logger.error(f"Load step failed: {error}")
        return results
    
    # -------------------------------------------------------------------------
    # PIPELINE SUMMARY
    # -------------------------------------------------------------------------
    
    print('-'*140)
    print("ETL PIPELINE COMPLETE")
    print('-'*140)
    print("SUMMARY:")
    print(f"  Rows read from CSV: {results['rows_read']}")
    print(f"  Rows after cleaning: {results['rows_after_cleaning']}")
    print(f"  Rows after transformation: {results['rows_after_transformation']}")
    print(f"  Rows loaded to MySQL: {results['rows_loaded']}")
    print(f"  Steps completed: {', '.join(results['steps_completed'])}")
    
    if results['success']:
        print(f"\nSTATUS: SUCCESS")
        print(f"Data is now available in your MySQL database")
        print(f"Table name: books")
        print(f"Database: business_intelligence")
    else:
        print(f"\nSTATUS: FAILED")
        print(f"Errors: {results['errors']}")
    
    print('-'*140)
    
    return results


# -----------------------------------------------------------------------------
# ASK USER FOR OPTIONS
# -----------------------------------------------------------------------------

print("\nETL PIPELINE CONFIGURATION")
print('-'*70)
print("Options:")
print("  1. Use latest CSV file (recommended)")
print("  2. Specify a specific CSV file")
print('-'*70)

file_choice = input("Enter choice (1 or 2, default 1): ").strip()

csv_path = None
if file_choice == '2':
    csv_path = input("Enter full path to CSV file: ").strip()
    if not csv_path:
        csv_path = None

print('-'*70)
print("Validation options:")
print("  1. Stop if validation fails (recommended)")
print("  2. Skip validation and load anyway")
print('-'*70)

validation_choice = input("Enter choice (1 or 2, default 1): ").strip()
skip_validation = (validation_choice == '2')

print('-'*70)
print("Duplicate handling:")
print("  1. Skip existing books (recommended)")
print("  2. Insert all books (may create duplicates)")
print('-'*70)

duplicate_choice = input("Enter choice (1 or 2, default 1): ").strip()
skip_duplicates = (duplicate_choice != '2')

print('-'*140)

# Confirm before running
print("\nCONFIGURATION SUMMARY:")
print(f"  CSV file: {csv_path if csv_path else 'Latest file in data/raw'}")
print(f"  Validation: {'SKIP' if skip_validation else 'ENFORCE'}")
print(f"  Duplicates: {'SKIP' if skip_duplicates else 'ALLOW'}")
print('-'*70)

confirm = input("Proceed with ETL pipeline? (yes/no): ").strip().lower()

if confirm not in ['yes', 'y']:
    print("ETL pipeline cancelled by user")
    logger.info("ETL pipeline cancelled by user")
    sys.exit(0)

# -----------------------------------------------------------------------------
# RUN THE PIPELINE
# -----------------------------------------------------------------------------

try:
    final_results = run_etl_pipeline(
        csv_file_path=csv_path,
        skip_validation=skip_validation,
        skip_duplicates=skip_duplicates
    )
    
    print('-'*140)
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Log file saved to: {log_filepath}")
    print('-'*140)
    
    if final_results['success']:
        print("\nNEXT STEPS:")
        print("  1. Verify data in MySQL: SELECT * FROM books;")
        print("  2. Proceed to Phase 3: Data Warehouse (advanced SQL)")
        print("  3. Proceed to Phase 4: Machine Learning models")
        print('-'*140)
    else:
        print("\nTROUBLESHOOTING:")
        print("  1. Check the log file for details")
        print("  2. Make sure MySQL is running")
        print("  3. Verify config.py has correct database credentials")
        print('-'*140)
        
except KeyboardInterrupt:
    print("\n\nETL pipeline interrupted by user (Ctrl+C)")
    logger.warning("ETL pipeline interrupted by user")
    sys.exit(1)

except Exception as error:
    print(f"\n\nUNEXPECTED ERROR: {error}")
    logger.error(f"Unexpected error: {error}", exc_info=True)
    sys.exit(1)

# Update output folder with fresh reports
update_outputs()

print("ETL pipeline execution complete")
print('-'*140)