"""
READ DATA MODULE - Reads CSV files and returns pandas DataFrame
"""

import pandas as pd
import logging
import re
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_RAW, DATA_PROCESSED

logger = logging.getLogger(__name__)


def get_latest_csv_file():
    """
    Find the most recent CSV file in the data/raw folder.
    IGNORES the master file (all_books_master.csv).
    Only looks for timestamped files (books_YYYYMMDD_HHMMSS.csv).
    """
    
    # Find all CSV files that match the timestamp pattern
    pattern = r"books_\d{8}_\d{6}\.csv"
    
    csv_files = []
    for file in DATA_RAW.glob("books_*.csv"):
        if re.match(pattern, file.name):
            csv_files.append(file)
    
    if not csv_files:
        logger.error("No timestamped CSV files found in data/raw folder")
        print("ERROR: No timestamped CSV files found. Run the scraper first.")
        return None
    
    # Find the most recent file by modification time
    latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)
    
    logger.info(f"Found {len(csv_files)} timestamped CSV files")
    logger.info(f"Latest file: {latest_file.name}")
    
    return latest_file


def read_csv_to_dataframe(file_path=None):
    """Read a CSV file and return a pandas DataFrame."""
    
    if file_path is None:
        file_path = get_latest_csv_file()
        if file_path is None:
            return None
    
    print('-'*140)
    print("READING CSV FILE")
    print('-'*140)
    print(f"File path: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        print(f"SUCCESS: Read {len(df)} rows and {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        return df
    except Exception as error:
        logger.error(f"Error reading CSV: {error}")
        return None


def save_processed_data(df, filename=None):
    """Save the cleaned DataFrame to the processed data folder."""
    
    if df is None or df.empty:
        return None
    
    if filename is None:
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"books_cleaned_{timestamp}.csv"
    
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    file_path = DATA_PROCESSED / filename
    
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
        logger.info(f"Saved processed data to {file_path}")
        print(f"Processed data saved to: {file_path}")
        return file_path
    except Exception as error:
        logger.error(f"Error saving processed data: {error}")
        return None


def get_data_info(df):
    """
    Print summary information about the DataFrame.
    """
    
    if df is None or df.empty:
        print("No data to analyze")
        return
    
    print('-'*140)
    print("DATA SUMMARY")
    print('-'*140)
    
    print(f"Number of rows: {len(df)}")
    print(f"Number of columns: {len(df.columns)}")
    print('-'*140)
    
    print("\nMISSING VALUES (null counts):")
    print('-'*140)
    missing = df.isnull().sum()
    print(missing[missing > 0] if any(missing > 0) else "No missing values found")
    print('-'*140)
    
    print("\nDATA TYPES:")
    print('-'*140)
    print(df.dtypes)
    print('-'*140)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = read_csv_to_dataframe()
    if df is not None:
        get_data_info(df)
        print(f"\nSUCCESS: Loaded {len(df)} rows")