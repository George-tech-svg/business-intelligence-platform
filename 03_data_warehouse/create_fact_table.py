"""
CREATE FACT TABLE - Builds the center of the star schema
This script creates the fact table that connects all dimension tables.
The fact table stores measurable numbers (facts) like price and rating.

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
print("PHASE 3: DATA WAREHOUSE - CREATING FACT TABLE")
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


def create_fact_table(cursor):
    """
    Create the fact table for book analysis.
    
    This table connects to:
    - dim_book (via book_id)
    - dim_rating (via rating_id)
    - dim_date (via date_id)
    - dim_price_category (via category_id)
    """
    
    sql = """
    CREATE TABLE IF NOT EXISTS fact_book_analysis (
        fact_id INT AUTO_INCREMENT PRIMARY KEY,
        book_id INT NOT NULL,
        rating_id INT NOT NULL,
        date_id INT NOT NULL,
        category_id INT NOT NULL,
        price_numeric DECIMAL(10, 2),
        rating_number INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_book_id (book_id),
        INDEX idx_rating_id (rating_id),
        INDEX idx_date_id (date_id),
        INDEX idx_category_id (category_id)
    )
    """
    
    try:
        cursor.execute(sql)
        print("SUCCESS: fact_book_analysis table created")
        return True
    except Error as error:
        print(f"ERROR creating fact table: {error}")
        return False


def add_foreign_keys(cursor):
    """
    Add foreign key constraints to link fact table with dimension tables.
    
    Foreign keys ensure data integrity - you cannot add a fact
    that references a book that does not exist.
    """
    
    # Foreign key to dim_book
    fk_book = """
    ALTER TABLE fact_book_analysis
    ADD CONSTRAINT fk_fact_book
    FOREIGN KEY (book_id) REFERENCES dim_book(book_id)
    ON DELETE RESTRICT
    """
    
    # Foreign key to dim_rating
    fk_rating = """
    ALTER TABLE fact_book_analysis
    ADD CONSTRAINT fk_fact_rating
    FOREIGN KEY (rating_id) REFERENCES dim_rating(rating_id)
    ON DELETE RESTRICT
    """
    
    # Foreign key to dim_date
    fk_date = """
    ALTER TABLE fact_book_analysis
    ADD CONSTRAINT fk_fact_date
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
    ON DELETE RESTRICT
    """
    
    # Foreign key to dim_price_category
    fk_category = """
    ALTER TABLE fact_book_analysis
    ADD CONSTRAINT fk_fact_category
    FOREIGN KEY (category_id) REFERENCES dim_price_category(category_id)
    ON DELETE RESTRICT
    """
    
    success = True
    
    try:
        cursor.execute(fk_book)
        print("SUCCESS: Added foreign key to dim_book")
    except Error as error:
        # Foreign key might already exist
        if "Duplicate" in str(error) or "already exists" in str(error):
            print("INFO: Foreign key to dim_book already exists")
        else:
            print(f"WARNING: Could not add foreign key to dim_book: {error}")
            success = False
    
    try:
        cursor.execute(fk_rating)
        print("SUCCESS: Added foreign key to dim_rating")
    except Error as error:
        if "Duplicate" in str(error) or "already exists" in str(error):
            print("INFO: Foreign key to dim_rating already exists")
        else:
            print(f"WARNING: Could not add foreign key to dim_rating: {error}")
            success = False
    
    try:
        cursor.execute(fk_date)
        print("SUCCESS: Added foreign key to dim_date")
    except Error as error:
        if "Duplicate" in str(error) or "already exists" in str(error):
            print("INFO: Foreign key to dim_date already exists")
        else:
            print(f"WARNING: Could not add foreign key to dim_date: {error}")
            success = False
    
    try:
        cursor.execute(fk_category)
        print("SUCCESS: Added foreign key to dim_price_category")
    except Error as error:
        if "Duplicate" in str(error) or "already exists" in str(error):
            print("INFO: Foreign key to dim_price_category already exists")
        else:
            print(f"WARNING: Could not add foreign key to dim_price_category: {error}")
            success = False
    
    return success


def show_fact_table_structure(cursor):
    """
    Display the structure of the fact table.
    """
    
    cursor.execute("DESCRIBE fact_book_analysis")
    columns = cursor.fetchall()
    
    print('-'*140)
    print("FACT TABLE STRUCTURE (fact_book_analysis):")
    print('-'*140)
    print(f"{'Field':<20} {'Type':<20} {'Null':<10} {'Key':<10}")
    print('-'*70)
    for col in columns:
        print(f"{col[0]:<20} {col[1]:<20} {col[2]:<10} {col[3]:<10}")
    print('-'*140)


def show_all_tables(cursor):
    """
    Display all tables in the database.
    """
    
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    
    print('-'*140)
    print("ALL TABLES IN BUSINESS_INTELLIGENCE DATABASE:")
    print('-'*140)
    for table in tables:
        print(f"  - {table[0]}")
    print('-'*140)


def main():
    """
    Main function to create the fact table.
    """
    
    connection = get_db_connection()
    
    if connection is None:
        print("ERROR: Could not connect to database")
        return False
    
    cursor = connection.cursor()
    
    print('-'*140)
    print("CREATING FACT TABLE")
    print('-'*140)
    
    # Create the fact table
    if not create_fact_table(cursor):
        connection.close()
        return False
    
    # Add foreign key constraints
    add_foreign_keys(cursor)
    
    connection.commit()
    
    # Show the table structure
    show_fact_table_structure(cursor)
    
    # Show all tables
    show_all_tables(cursor)
    
    cursor.close()
    connection.close()
    
    print('-'*140)
    print("STATUS: SUCCESS - Fact table created")
    print("Table created: fact_book_analysis")
    print('-'*140)
    
    return True


if __name__ == "__main__":
    main()