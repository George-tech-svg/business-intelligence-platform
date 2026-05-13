"""
CREATE DIMENSION TABLES - Builds the star schema dimension tables
This script creates dimension tables for the data warehouse.
Dimension tables store descriptive information like book details,
rating categories, and date information.

Author: Business Intelligence Platform
Date: 2026-05-12
"""

import mysql.connector
from mysql.connector import Error
import logging
import sys
from pathlib import Path

# Add project root to path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import LOCAL_DB_CONFIG, ACTIVE_DB

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print('-'*140)
print("PHASE 3: DATA WAREHOUSE - CREATING DIMENSION TABLES")
print('-'*140)


def get_db_connection():
    """
    Create and return a connection to the MySQL database.
    """
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


def create_dim_book_table(cursor):
    """
    Create the book dimension table.
    This table stores information about each book.
    """
    
    sql = """
    CREATE TABLE IF NOT EXISTS dim_book (
        book_id INT AUTO_INCREMENT PRIMARY KEY,
        book_title VARCHAR(500) NOT NULL,
        searchable_title VARCHAR(500),
        original_price DECIMAL(10, 2),
        scraped_at DATETIME,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_title (book_title(100))
    )
    """
    
    try:
        cursor.execute(sql)
        print("SUCCESS: dim_book table created")
        return True
    except Error as error:
        print(f"ERROR creating dim_book: {error}")
        return False


def create_dim_rating_table(cursor):
    """
    Create the rating dimension table.
    This table stores rating categories.
    """
    
    sql = """
    CREATE TABLE IF NOT EXISTS dim_rating (
        rating_id INT AUTO_INCREMENT PRIMARY KEY,
        rating_name VARCHAR(20),
        rating_number INT,
        rating_category VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    try:
        cursor.execute(sql)
        print("SUCCESS: dim_rating table created")
        return True
    except Error as error:
        print(f"ERROR creating dim_rating: {error}")
        return False


def create_dim_date_table(cursor):
    """
    Create the date dimension table.
    This table stores date information for time-based analysis.
    """
    
    sql = """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INT PRIMARY KEY,
        full_date DATE,
        year INT,
        month INT,
        month_name VARCHAR(20),
        day INT,
        day_name VARCHAR(10),
        quarter INT,
        week_number INT,
        is_weekend BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    try:
        cursor.execute(sql)
        print("SUCCESS: dim_date table created")
        return True
    except Error as error:
        print(f"ERROR creating dim_date: {error}")
        return False


def create_dim_price_category_table(cursor):
    """
    Create the price category dimension table.
    This table stores price categories for analysis.
    """
    
    sql = """
    CREATE TABLE IF NOT EXISTS dim_price_category (
        category_id INT AUTO_INCREMENT PRIMARY KEY,
        category_name VARCHAR(20),
        min_price DECIMAL(10, 2),
        max_price DECIMAL(10, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    try:
        cursor.execute(sql)
        print("SUCCESS: dim_price_category table created")
        
        # Insert price category definitions
        insert_sql = """
        INSERT IGNORE INTO dim_price_category (category_id, category_name, min_price, max_price)
        VALUES 
            (1, 'Budget', 0, 20),
            (2, 'Mid', 20, 40),
            (3, 'Premium', 40, 1000)
        """
        cursor.execute(insert_sql)
        print("  Added price category definitions")
        return True
    except Error as error:
        print(f"ERROR creating dim_price_category: {error}")
        return False


def populate_dim_rating_table(cursor):
    """
    Insert rating data into the rating dimension table.
    """
    
    sql = """
    INSERT IGNORE INTO dim_rating (rating_id, rating_name, rating_number, rating_category)
    VALUES 
        (1, 'One', 1, 'Low'),
        (2, 'Two', 2, 'Low'),
        (3, 'Three', 3, 'Medium'),
        (4, 'Four', 4, 'High'),
        (5, 'Five', 5, 'High')
    """
    
    try:
        cursor.execute(sql)
        print("SUCCESS: Rating data inserted into dim_rating")
        return True
    except Error as error:
        print(f"ERROR populating dim_rating: {error}")
        return False


def show_tables(cursor):
    """
    Display all tables in the database to verify creation.
    """
    
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    
    print('-'*140)
    print("TABLES IN BUSINESS_INTELLIGENCE DATABASE:")
    print('-'*140)
    for table in tables:
        print(f"  - {table[0]}")
    print('-'*140)


def main():
    """
    Main function to create all dimension tables.
    """
    
    connection = get_db_connection()
    
    if connection is None:
        print("ERROR: Could not connect to database")
        return False
    
    cursor = connection.cursor()
    
    print('-'*140)
    print("CREATING DIMENSION TABLES")
    print('-'*140)
    
    # Create all dimension tables
    success = True
    
    if not create_dim_book_table(cursor):
        success = False
    
    if not create_dim_rating_table(cursor):
        success = False
    
    if not create_dim_date_table(cursor):
        success = False
    
    if not create_dim_price_category_table(cursor):
        success = False
    
    # Populate rating table with data
    if not populate_dim_rating_table(cursor):
        success = False
    
    connection.commit()
    
    # Show all tables
    show_tables(cursor)
    
    cursor.close()
    connection.close()
    
    print('-'*140)
    if success:
        print("STATUS: SUCCESS - All dimension tables created")
        print("Tables created: dim_book, dim_rating, dim_date, dim_price_category")
    else:
        print("STATUS: PARTIAL SUCCESS - Some tables may have errors")
    
    print('-'*140)
    
    return success


if __name__ == "__main__":
    main()