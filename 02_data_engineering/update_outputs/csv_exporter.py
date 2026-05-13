"""
CSV EXPORTER - Saves book data to CSV files
"""

import pandas as pd
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import OUTPUT_EXPORTS


def export_to_csv(df):
    """
    Export book data to CSV file.
    Creates timestamped file and latest file.
    """
    
    if df is None or df.empty:
        print("No data to export")
        return None
    
    OUTPUT_EXPORTS.mkdir(parents=True, exist_ok=True)
    
    # Timestamped file (keeps history)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = OUTPUT_EXPORTS / f"books_export_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    print(f"  Timestamped CSV: {csv_path}")
    
    # Latest file (overwrites)
    latest_path = OUTPUT_EXPORTS / "books_export_latest.csv"
    df.to_csv(latest_path, index=False)
    print(f"  Latest CSV: {latest_path}")
    
    return csv_path