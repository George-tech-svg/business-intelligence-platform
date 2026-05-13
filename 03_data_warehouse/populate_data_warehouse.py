"""
POPULATE DATA WAREHOUSE - Loads data into dimension and fact tables
This script takes data from the books table and loads it into:
- dim_date (unique dates)
- dim_book (unique books)
- fact_book_analysis (links to all dimensions)

Author: Business Intelligence Platform
Date: 2026-05-12
"""

import mysql.connector
from mysql.connector import Error
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import LOCAL_DB_CONFIG, ACTIVE_DB

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print('-'*140)
print("PHASE 3: DATA WAREHOUSE - POPULATING TABLES")
print('-'*140)


def get_db_connection():
    """Create database connection."""
    if ACTIVE_DB == 'local':
        db_config = LOCAL_DB_CONFIG
    else:
        print("ERROR: Cloud database not configured")
        return None
    
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print(f"Connected to database: {db_config['database']}")
            return connection
    except Error as error:
        print(f"Connection error: {error}")
        return None


def populate_dim_date(cursor):
    """
    Populate the date dimension table with unique dates from books table.
    """
    
    print('-'*70)
    print("POPULATING dim_date TABLE")
    print('-'*70)
    
    sql = """
    INSERT IGNORE INTO dim_date (date_id, full_date, year, month, day)
    SELECT DISTINCT
        CAST(REPLACE(LEFT(scraped_at, 10), '-', '') AS UNSIGNED) as date_id,
        DATE(scraped_at) as full_date,
        YEAR(scraped_at) as year,
        MONTH(scraped_at) as month,
        DAY(scraped_at) as day
    FROM books
    WHERE scraped_at IS NOT NULL
    """
    
    try:
        cursor.execute(sql)
        count = cursor.rowcount
        print(f"SUCCESS: Inserted {count} unique dates into dim_date")
        return True
    except Error as error:
        print(f"ERROR populating dim_date: {error}")
        return False


def populate_dim_book(cursor):
    """
    Populate the book dimension table with unique books from books table.
    """
    
    print('-'*70)
    print("POPULATING dim_book TABLE")
    print('-'*70)
    
    sql = """
    INSERT IGNORE INTO dim_book (book_title, original_price, scraped_at)
    SELECT DISTINCT
        title,
        price_numeric,
        scraped_at
    FROM books
    WHERE title IS NOT NULL
    """
    
    try:
        cursor.execute(sql)
        count = cursor.rowcount
        print(f"SUCCESS: Inserted {count} unique books into dim_book")
        return True
    except Error as error:
        print(f"ERROR populating dim_book: {error}")
        return False


def populate_fact_table(cursor):
    """
    Populate the fact table by joining data from all tables.
    FIXED: Using 'rating_text' column name from books table.
    """
    
    print('-'*70)
    print("POPULATING fact_book_analysis TABLE")
    print('-'*70)
    
    # SQL to insert facts using correct column name: rating_text
    sql = """
    INSERT IGNORE INTO fact_book_analysis 
    (book_id, rating_id, date_id, category_id, price_numeric, rating_number)
    SELECT 
        db.book_id,
        dr.rating_id,
        dd.date_id,
        dpc.category_id,
        b.price_numeric,
        dr.rating_number
    FROM books b
    INNER JOIN dim_book db ON db.book_title = b.title
    INNER JOIN dim_rating dr ON dr.rating_name = b.rating_text
    INNER JOIN dim_date dd ON dd.full_date = DATE(b.scraped_at)
    INNER JOIN dim_price_category dpc ON 
        (dpc.category_name = 'Budget' AND b.price_numeric BETWEEN dpc.min_price AND dpc.max_price)
        OR (dpc.category_name = 'Mid' AND b.price_numeric BETWEEN dpc.min_price AND dpc.max_price)
        OR (dpc.category_name = 'Premium' AND b.price_numeric BETWEEN dpc.min_price AND dpc.max_price)
    """
    
    try:
        cursor.execute(sql)
        count = cursor.rowcount
        print(f"SUCCESS: Inserted {count} fact records into fact_book_analysis")
        return True
    except Error as error:
        print(f"ERROR populating fact table: {error}")
        print("TROUBLESHOOTING: Check that column names match exactly")
        return False


def verify_population(cursor):
    """Verify that all tables have been populated correctly."""
    
    print('-'*70)
    print("VERIFICATION - TABLE ROW COUNTS")
    print('-'*70)
    
    tables = ['dim_book', 'dim_rating', 'dim_date', 'dim_price_category', 'fact_book_analysis']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} rows")
    
    print('-'*70)


def show_sample_fact_data(cursor):
    """Display sample data from the fact table with all dimension information."""
    
    print('-'*70)
    print("SAMPLE STAR SCHEMA DATA (First 5 records)")
    print('-'*70)
    
    sql = """
    SELECT 
        f.fact_id,
        db.book_title,
        dr.rating_name,
        dr.rating_number,
        dd.full_date,
        dpc.category_name,
        f.price_numeric
    FROM fact_book_analysis f
    INNER JOIN dim_book db ON f.book_id = db.book_id
    INNER JOIN dim_rating dr ON f.rating_id = dr.rating_id
    INNER JOIN dim_date dd ON f.date_id = dd.date_id
    INNER JOIN dim_price_category dpc ON f.category_id = dpc.category_id
    LIMIT 5
    """
    
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        
        if not results:
            print("No data found in fact table yet.")
            return
        
        print(f"{'ID':<5} {'Book Title':<40} {'Rating':<10} {'Stars':<6} {'Date':<12} {'Category':<10} {'Price':<8}")
        print('-'*120)
        
        for row in results:
            fact_id = row[0]
            title = row[1][:38] + '..' if len(row[1]) > 40 else row[1]
            rating_name = row[2]
            stars = row[3]
            date_val = row[4]
            category = row[5]
            price = row[6]
            
            print(f"{fact_id:<5} {title:<40} {rating_name:<10} {stars:<6} {date_val:<12} {category:<10} ${price:<7.2f}")
        
        print('-'*120)
        
    except Error as error:
        print(f"ERROR showing sample data: {error}")


def main():
    """Main function to populate the data warehouse."""
    
    connection = get_db_connection()
    
    if connection is None:
        print("ERROR: Could not connect to database")
        return False
    
    cursor = connection.cursor()
    
    print('-'*140)
    print("STARTING DATA WAREHOUSE POPULATION")
    print('-'*140)
    
    # Step 1: Populate date dimension
    if not populate_dim_date(cursor):
        connection.rollback()
        connection.close()
        return False
    
    # Step 2: Populate book dimension
    if not populate_dim_book(cursor):
        connection.rollback()
        connection.close()
        return False
    
    # Step 3: Populate fact table
    if not populate_fact_table(cursor):
        connection.rollback()
        connection.close()
        return False
    
    # Commit all changes
    connection.commit()
    
    # Verify population
    verify_population(cursor)
    
    # Show sample data
    show_sample_fact_data(cursor)
    
    cursor.close()
    connection.close()
    
    print('-'*140)
    print("STATUS: SUCCESS - Data warehouse populated")
    print("All dimension and fact tables now contain data")
    print('-'*140)
    
    return True


if __name__ == "__main__":
    main()