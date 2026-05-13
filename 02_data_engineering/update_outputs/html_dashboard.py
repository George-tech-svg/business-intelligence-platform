"""
HTML DASHBOARD - Creates static HTML dashboard
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import OUTPUT_DASHBOARDS


def create_html_dashboard(df):
    """
    Create a static HTML dashboard that can be viewed in any browser.
    Creates timestamped HTML and latest HTML.
    """
    
    if df is None or df.empty:
        print("No data for HTML dashboard")
        return None
    
    OUTPUT_DASHBOARDS.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Calculate statistics
    total_books = len(df)
    avg_price = df['price_numeric'].mean()
    min_price = df['price_numeric'].min()
    max_price = df['price_numeric'].max()
    avg_rating = df['rating_number'].mean()
    
    # Group by rating
    rating_counts = df['rating_name'].value_counts().to_dict()
    price_by_rating = df.groupby('rating_name')['price_numeric'].mean().round(2).to_dict()
    
    # Group by price category
    category_counts = df['price_category'].value_counts().to_dict()
    
    # Top 10 expensive books
    top_expensive = df.nlargest(10, 'price_numeric')[['book_title', 'price_numeric', 'rating_name']]
    
    # Best value books
    high_rated = df[df['rating_number'] >= 4]
    if len(high_rated) > 0:
        best_value = high_rated.nsmallest(10, 'price_numeric')[['book_title', 'price_numeric', 'rating_name']]
    else:
        best_value = pd.DataFrame()
    
    # Build HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Business Intelligence Dashboard</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f0f2f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        h1 {{
            color: #1a1a2e;
            text-align: center;
        }}
        .subtitle {{
            text-align: center;
            color: #666;
            margin-bottom: 30px;
        }}
        .metrics {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .metric-card {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        .metric-value {{
            font-size: 32px;
            font-weight: bold;
            color: #007bff;
        }}
        .metric-label {{
            color: #666;
            margin-top: 10px;
        }}
        .section {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        .section h2 {{
            margin-top: 0;
            color: #1a1a2e;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #007bff;
            color: white;
        }}
        .two-columns {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        .footer {{
            text-align: center;
            color: #666;
            margin-top: 30px;
            padding: 20px;
        }}
        @media (max-width: 768px) {{
            .two-columns {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Business Intelligence Dashboard</h1>
        <div class="subtitle">Complete Data Pipeline | Updated: {timestamp}</div>
        
        <div class="metrics">
            <div class="metric-card">
                <div class="metric-value">{total_books}</div>
                <div class="metric-label">Total Books</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">${avg_price:.2f}</div>
                <div class="metric-label">Average Price</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">${min_price:.2f} - ${max_price:.2f}</div>
                <div class="metric-label">Price Range</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{avg_rating:.1f}</div>
                <div class="metric-label">Average Rating</div>
            </div>
        </div>
        
        <div class="two-columns">
            <div class="section">
                <h2>Books by Rating</h2>
                <table>
                    <tr><th>Rating</th><th>Count</th></tr>
"""
    
    rating_order = ['Five', 'Four', 'Three', 'Two', 'One']
    for rating in rating_order:
        count = rating_counts.get(rating, 0)
        html += f"                    <tr><td>{rating} Star</td><td>{count}</td></tr>\n"
    
    html += f"""
                </table>
            </div>
            
            <div class="section">
                <h2>Books by Price Category</h2>
                <table>
                    <tr><th>Category</th><th>Count</th></tr>
                    <tr><td>Budget (under $20)</td><td>{category_counts.get('Budget', 0)}</td></tr>
                    <tr><td>Mid ($20-$40)</td><td>{category_counts.get('Mid', 0)}</td></tr>
                    <tr><td>Premium (over $40)</td><td>{category_counts.get('Premium', 0)}</td></tr>
                </table>
            </div>
        </div>
        
        <div class="section">
            <h2>Average Price by Rating</h2>
            <table>
                <tr><th>Rating</th><th>Average Price</th></tr>
"""
    
    for rating in rating_order:
        avg = price_by_rating.get(rating, 0)
        html += f"                <tr><td>{rating} Star</td><td>${avg:.2f}</td></tr>\n"
    
    html += f"""
            </table>
        </div>
        
        <div class="two-columns">
            <div class="section">
                <h2>Top 10 Most Expensive Books</h2>
                <table>
                    <tr><th>Book Title</th><th>Price</th><th>Rating</th></tr>
"""
    
    for _, row in top_expensive.iterrows():
        title = row['book_title'][:40] + '..' if len(row['book_title']) > 40 else row['book_title']
        html += f"                    <tr><td>{title}</td><td>${row['price_numeric']:.2f}</td><td>{row['rating_name']}</td></tr>\n"
    
    html += f"""
                </table>
            </div>
            
            <div class="section">
                <h2>Best Value Books (4-5 Stars)</h2>
                <table>
                    <tr><th>Book Title</th><th>Price</th><th>Rating</th></tr>
"""
    
    if len(best_value) > 0:
        for _, row in best_value.iterrows():
            title = row['book_title'][:40] + '..' if len(row['book_title']) > 40 else row['book_title']
            html += f"                    <tr><td>{title}</td><td>${row['price_numeric']:.2f}</td><td>{row['rating_name']}</td></tr>\n"
    else:
        html += "                    <tr><td colspan='3'>No high-rated books found</td></tr>\n"
    
    html += f"""
                </table>
            </div>
        </div>
        
        <div class="footer">
            Business Intelligence Platform | Data Pipeline Automation
        </div>
    </div>
</body>
</html>
"""
    
    # Timestamped HTML (keeps history)
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    html_path = OUTPUT_DASHBOARDS / f"dashboard_{timestamp_str}.html"
    with open(html_path, 'w') as f:
        f.write(html)
    print(f"  Timestamped HTML: {html_path}")
    
    # Latest HTML (overwrites)
    latest_html_path = OUTPUT_DASHBOARDS / "dashboard_latest.html"
    with open(latest_html_path, 'w') as f:
        f.write(html)
    print(f"  Latest HTML: {latest_html_path}")
    
    return html_path