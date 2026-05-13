"""
RUN UPDATES - Orchestrates all output generation
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from data_loader import get_data_from_database
from csv_exporter import export_to_csv
from report_generator import generate_report
from html_dashboard import create_html_dashboard

print('-'*140)
print("OUTPUT FOLDER AUTOMATION")
print('-'*140)


def main():
    """
    Run all output generation functions in order.
    """
    
    print("Loading data from database...")
    df = get_data_from_database()
    
    if df is None or df.empty:
        print("ERROR: No data found in database")
        return False
    
    print(f"Processing {len(df)} books")
    print('-'*70)
    
    # Generate all outputs
    export_to_csv(df)
    generate_report(df)
    create_html_dashboard(df)
    
    print('-'*70)
    print("OUTPUT FOLDER UPDATE COMPLETE")
    print("  - CSV: output/exports/")
    print("  - Reports: output/reports/")
    print("  - HTML Dashboard: output/dashboards/")
    print('-'*140)
    
    return True


if __name__ == "__main__":
    main()