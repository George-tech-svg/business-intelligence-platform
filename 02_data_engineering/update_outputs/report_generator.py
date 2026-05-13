"""
REPORT GENERATOR - Creates text summary report
"""

import pandas as pd
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import OUTPUT_REPORTS


def generate_report(df):
    """
    Generate a text summary report with key metrics.
    Creates timestamped report and latest report.
    """
    
    if df is None or df.empty:
        print("No data for report")
        return None
    
    OUTPUT_REPORTS.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Calculate statistics
    total_books = len(df)
    avg_price = df['price_numeric'].mean()
    min_price = df['price_numeric'].min()
    max_price = df['price_numeric'].max()
    avg_rating = df['rating_number'].mean()
    
    # Group by price category
    price_category_counts = df['price_category'].value_counts()
    
    # Group by rating
    rating_counts = df['rating_name'].value_counts()
    
    # Price by rating
    price_by_rating = df.groupby('rating_name')['price_numeric'].agg(['count', 'min', 'max', 'mean']).round(2)
    
    # Top 5 most expensive
    top_expensive = df.nlargest(5, 'price_numeric')[['book_title', 'price_numeric', 'rating_name']]
    
    # Best value (high rating, low price)
    high_rated = df[df['rating_number'] >= 4]
    if len(high_rated) > 0:
        best_value = high_rated.nsmallest(5, 'price_numeric')[['book_title', 'price_numeric', 'rating_name']]
    else:
        best_value = pd.DataFrame()
    
    # Build the report
    report = f"""
================================================================================
BUSINESS INTELLIGENCE PLATFORM - SUMMARY REPORT
================================================================================
Report generated: {timestamp}

================================================================================
KEY METRICS
================================================================================
Total Books: {total_books}
Average Price: ${avg_price:.2f}
Price Range: ${min_price:.2f} - ${max_price:.2f}
Average Rating: {avg_rating:.1f} stars

================================================================================
BOOKS BY PRICE CATEGORY
================================================================================
{price_category_counts.to_string()}

================================================================================
BOOKS BY RATING
================================================================================
{rating_counts.to_string()}

================================================================================
PRICE STATISTICS BY RATING
================================================================================
{price_by_rating.to_string()}

================================================================================
TOP 5 MOST EXPENSIVE BOOKS
================================================================================
{top_expensive.to_string(index=False) if len(top_expensive) > 0 else 'No data'}

================================================================================
BEST VALUE BOOKS (4-5 Stars, Lowest Price)
================================================================================
{best_value.to_string(index=False) if len(best_value) > 0 else 'No high-rated books found'}

================================================================================
END OF REPORT
================================================================================
"""
    
    # Timestamped report (keeps history)
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = OUTPUT_REPORTS / f"summary_report_{timestamp_str}.txt"
    with open(report_path, 'w') as f:
        f.write(report)
    print(f"  Timestamped report: {report_path}")
    
    # Latest report (overwrites)
    latest_report_path = OUTPUT_REPORTS / "summary_report_latest.txt"
    with open(latest_report_path, 'w') as f:
        f.write(report)
    print(f"  Latest report: {latest_report_path}")
    
    return report_path