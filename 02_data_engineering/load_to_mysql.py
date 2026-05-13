"""
LOAD TO MYSQL MODULE - Loads cleaned data into MySQL database
This module takes the cleaned and transformed DataFrame and loads it
into the MySQL database. It creates the table if it doesn't exist
and handles errors properly.

Author: Business Intelligence Platform
Date: 2026-05-12
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd
import logging
from pathlib import Path
import sys

# Add project root to path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import LOCAL_DB_CONFIG, ACTIVE_DB

# Setup logger for this module
logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Create and return a connection to the MySQL database.
    
    Uses the configuration from config.py.
    Automatically selects local or cloud based on ACTIVE_DB setting.
    
    Returns:
        MySQL connection object if successful, None if failed
    """
    
    # Choose which database config to use
    if ACTIVE_DB == 'local':
        db_config = LOCAL_DB_CONFIG
        logger.info(f"Using LOCAL database at {db_config['host']}")
    else:
        # Cloud config would be used here
        logger.error("Cloud database not configured yet. Use local for now.")
        return None
    
    try:
        connection = mysql.connector.connect(**db_config)
        
        if connection.is_connected():
            logger.info(f"Connected to MySQL database: {db_config['database']}")
            return connection
            
    except Error as error:
        logger.error(f"Failed to connect to MySQL: {error}")
        return None


def create_books_table(connection):
    """
    Create the books table in the database if it does not exist.
    
    This table stores all book information from the scraper.
    Each column has an appropriate data type and constraints.
    
    Parameters:
        connection: Active MySQL connection
    
    Returns:
        True if successful, False if failed
    """
    
    cursor = None
    
    try:
        cursor = connection.cursor()
        
        # SQL statement to create the books table
        # IF NOT EXISTS means it won't error if table already exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS books (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(500) NOT NULL,
            price_original VARCHAR(50),
            price_numeric DECIMAL(10, 2),
            rating_text VARCHAR(20),
            rating_numeric INT,
            availability VARCHAR(100),
            price_category VARCHAR(20),
            rating_category VARCHAR(20),
            scraped_at DATETIME,
            scraped_year INT,
            scraped_month INT,
            scraped_day INT,
            searchable_title VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        connection.commit()
        
        logger.info("Books table created or already exists")
        print("SUCCESS: Books table is ready")
        
        # Also create an index on title for faster searching
        index_sql = """
        CREATE INDEX idx_title ON books(title(100))
        """
        try:
            cursor.execute(index_sql)
            connection.commit()
            logger.info("Created index on title column")
        except Error as e:
            # Index might already exist, that's fine
            logger.info(f"Index may already exist: {e}")
        
        return True
        
    except Error as error:
        logger.error(f"Failed to create books table: {error}")
        return False
        
    finally:
        if cursor:
            cursor.close()


def insert_books_data(connection, df):
    """
    Insert book data into the database.
    
    This function inserts rows one by one to handle errors gracefully.
    If one row fails, others still get inserted.
    
    Parameters:
        connection: Active MySQL connection
        df: pandas DataFrame with cleaned book data
    
    Returns:
        Dictionary with insert results (success_count, failed_count, errors)
    """
    
    results = {
        'success_count': 0,
        'failed_count': 0,
        'errors': []
    }
    
    if df is None or df.empty:
        logger.warning("No data to insert")
        results['errors'].append("DataFrame is empty")
        return results
    
    cursor = None
    
    try:
        cursor = connection.cursor()
        
        # SQL insert statement
        insert_sql = """
        INSERT INTO books (
            title, price_original, price_numeric, rating_text, rating_numeric,
            availability, price_category, rating_category, scraped_at,
            scraped_year, scraped_month, scraped_day, searchable_title
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # Loop through each row in the DataFrame
        for index, row in df.iterrows():
            try:
                # Prepare values for insertion
                values = (
                    row.get('title', ''),
                    row.get('price', ''),
                    row.get('price_numeric', None),
                    row.get('rating', ''),
                    row.get('rating_numeric', None),
                    row.get('availability', ''),
                    row.get('price_category', ''),
                    row.get('rating_category', ''),
                    row.get('scraped_date', None),
                    row.get('scraped_year', None),
                    row.get('scraped_month', None),
                    row.get('scraped_day', None),
                    row.get('searchable_title', '')
                )
                
                cursor.execute(insert_sql, values)
                results['success_count'] += 1
                
                # Print progress every 10 rows
                if results['success_count'] % 10 == 0:
                    print(f"  Inserted {results['success_count']} rows...")
                
            except Error as row_error:
                results['failed_count'] += 1
                error_msg = f"Row {index}: {row_error}"
                results['errors'].append(error_msg)
                logger.warning(f"Failed to insert row {index}: {row_error}")
        
        # Commit all successful inserts
        connection.commit()
        
        logger.info(f"Inserted {results['success_count']} rows, failed {results['failed_count']}")
        
    except Error as error:
        logger.error(f"Database error during insert: {error}")
        results['errors'].append(f"Database error: {error}")
        
    finally:
        if cursor:
            cursor.close()
    
    return results


def get_existing_titles(connection):
    """
    Get list of titles already in the database to avoid duplicates.
    
    Parameters:
        connection: Active MySQL connection
    
    Returns:
        Set of existing titles
    """
    
    existing_titles = set()
    cursor = None
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT title FROM books")
        
        for row in cursor.fetchall():
            existing_titles.add(row[0])
            
        logger.info(f"Found {len(existing_titles)} existing titles in database")
        
    except Error as error:
        logger.warning(f"Could not fetch existing titles: {error}")
        
    finally:
        if cursor:
            cursor.close()
    
    return existing_titles


def filter_new_books(df, connection):
    """
    Filter DataFrame to only include books not already in database.
    
    This prevents duplicate inserts when running the ETL multiple times.
    
    Parameters:
        df: pandas DataFrame with book data
        connection: Active MySQL connection
    
    Returns:
        DataFrame with only new books
    """
    
    if df is None or df.empty:
        return df
    
    existing_titles = get_existing_titles(connection)
    
    if not existing_titles:
        logger.info("No existing books found. All books are new.")
        return df
    
    # Filter out books that already exist
    df_new = df[~df['title'].isin(existing_titles)]
    
    duplicates_count = len(df) - len(df_new)
    logger.info(f"Found {duplicates_count} duplicate books that will be skipped")
    
    if duplicates_count > 0:
        print(f"Skipping {duplicates_count} existing books (not inserting duplicates)")
    
    return df_new


def run_full_load(df, skip_duplicates=True):
    """
    Run the complete loading process.
    
    This is the main function that orchestrates database loading.
    
    Parameters:
        df: pandas DataFrame with cleaned and transformed data
        skip_duplicates: Whether to skip books already in database
    
    Returns:
        Dictionary with loading results
    """
    
    results = {
        'success': False,
        'rows_inserted': 0,
        'errors': [],
        'message': ''
    }
    
    print('-'*140)
    print("LOADING DATA TO MYSQL DATABASE")
    print('-'*140)
    
    if df is None or df.empty:
        results['message'] = 'No data to load'
        results['errors'].append('DataFrame is empty')
        print("ERROR: No data to load")
        return results
    
    print(f"Preparing to load {len(df)} books into database")
    
    # Step 1: Connect to database
    print("Step 1: Connecting to MySQL database...")
    connection = get_db_connection()
    
    if connection is None:
        results['message'] = 'Database connection failed'
        results['errors'].append('Could not connect to MySQL')
        print("ERROR: Could not connect to database")
        return results
    
    print("SUCCESS: Connected to database")
    
    try:
        # Step 2: Create table if not exists
        print("Step 2: Ensuring books table exists...")
        if not create_books_table(connection):
            results['message'] = 'Failed to create table'
            results['errors'].append('Table creation failed')
            return results
        
        # Step 3: Filter out duplicates if requested
        if skip_duplicates:
            print("Step 3: Checking for existing books...")
            df_to_load = filter_new_books(df, connection)
        else:
            df_to_load = df
        
        if df_to_load.empty:
            results['success'] = True
            results['message'] = 'No new books to insert (all already in database)'
            print("SUCCESS: No new books to insert")
            return results
        
        print(f"Step 3: Will insert {len(df_to_load)} new books")
        
        # Step 4: Insert data
        print("Step 4: Inserting data into database...")
        insert_results = insert_books_data(connection, df_to_load)
        
        results['rows_inserted'] = insert_results['success_count']
        results['errors'] = insert_results['errors']
        
        if insert_results['failed_count'] > 0:
            results['success'] = False
            results['message'] = f"Inserted {insert_results['success_count']} rows, failed {insert_results['failed_count']}"
            print(f"WARNING: {insert_results['failed_count']} rows failed to insert")
        else:
            results['success'] = True
            results['message'] = f"Successfully loaded {insert_results['success_count']} books"
            print(f"SUCCESS: Loaded {insert_results['success_count']} books into database")
        
    except Exception as error:
        results['message'] = f"Loading failed: {error}"
        results['errors'].append(str(error))
        logger.error(f"Loading failed: {error}")
        
    finally:
        # Close the database connection
        if connection and connection.is_connected():
            connection.close()
            logger.info("Database connection closed")
    
    print('-'*140)
    print("LOADING COMPLETE")
    print(f"Result: {results['message']}")
    print('-'*140)
    
    return results


# This code runs only when you run this file directly
if __name__ == "__main__":
    print('-'*140)
    print("TESTING load_to_mysql.py MODULE")
    print('-'*140)
    
    # Setup basic logging
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data for testing
    sample_data = pd.DataFrame([
        {
            'title': 'Test Book 1',
            'price': '25.99',
            'price_numeric': 25.99,
            'rating': 'Four',
            'rating_numeric': 4,
            'availability': 'In stock',
            'price_category': 'Mid',
            'rating_category': 'High',
            'scraped_date': pd.Timestamp('2026-05-12 14:58:01'),
            'scraped_year': 2026,
            'scraped_month': 5,
            'scraped_day': 12,
            'searchable_title': 'test book 1'
        },
        {
            'title': 'Test Book 2',
            'price': '15.50',
            'price_numeric': 15.50,
            'rating': 'Three',
            'rating_numeric': 3,
            'availability': 'In stock',
            'price_category': 'Budget',
            'rating_category': 'Medium',
            'scraped_date': pd.Timestamp('2026-05-12 14:58:01'),
            'scraped_year': 2026,
            'scraped_month': 5,
            'scraped_day': 12,
            'searchable_title': 'test book 2'
        }
    ])
    
    print(f"Sample data has {len(sample_data)} rows for testing")
    print('-'*140)
    print("NOTE: This test will actually connect to your MySQL database")
    print("      and insert test data into the books table.")
    print('-'*140)
    
    confirm = input("Run test and insert sample data? (yes/no): ").strip().lower()
    
    if confirm in ['yes', 'y']:
        result = run_full_load(sample_data, skip_duplicates=True)
        
        print("\n" + '='*140)
        if result['success']:
            print("TEST SUCCESS: Data loaded to database")
            print(f"Rows inserted: {result['rows_inserted']}")
        else:
            print("TEST FAILED: Could not load data")
            print(f"Error: {result['message']}")
        print('='*140)
    else:
        print("Test cancelled by user")
    
    print('-'*140)