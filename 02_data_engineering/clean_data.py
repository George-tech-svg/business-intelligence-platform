"""
CLEAN DATA MODULE - Cleans and deduplicates the scraped data
This module removes duplicates, fixes missing values, and standardizes text.
Cleaning is essential before loading data into a database.

Author: Business Intelligence Platform
Date: 2026-05-12
"""

import pandas as pd
import logging
import re

# Setup logger for this module
logger = logging.getLogger(__name__)


def remove_duplicates(df, column='title'):
    """
    Remove duplicate rows based on a specific column.
    
    Duplicates happen when the same book appears on multiple pages
    or when the scraper runs multiple times.
    
    Parameters:
        df: pandas DataFrame to clean
        column: Column name to check for duplicates (default is 'title')
    
    Returns:
        DataFrame with duplicates removed
    """
    
    if df is None or df.empty:
        logger.warning("No data to clean")
        return df
    
    original_count = len(df)
    
    # Count duplicates before removal
    duplicates = df.duplicated(subset=[column], keep='first').sum()
    
    if duplicates > 0:
        # Remove duplicates, keeping the first occurrence
        df_cleaned = df.drop_duplicates(subset=[column], keep='first')
        logger.info(f"Removed {duplicates} duplicate books based on '{column}'")
    else:
        df_cleaned = df
        logger.info(f"No duplicates found in '{column}' column")
    
    logger.info(f"Rows before: {original_count}, after: {len(df_cleaned)}")
    
    return df_cleaned


def fix_missing_values(df):
    """
    Handle missing or null values in the DataFrame.
    
    For critical columns (title, price), we drop the row.
    For less critical columns, we fill with default values.
    
    Parameters:
        df: pandas DataFrame to clean
    
    Returns:
        DataFrame with missing values handled
    """
    
    if df is None or df.empty:
        return df
    
    original_count = len(df)
    
    # Check for missing values in each column
    missing_counts = df.isnull().sum()
    logger.info(f"Missing values before fixing: {missing_counts[missing_counts > 0].to_dict()}")
    
    # For title column - drop rows with missing title (critical)
    if 'title' in df.columns:
        before = len(df)
        df = df.dropna(subset=['title'])
        after = len(df)
        if before > after:
            logger.info(f"Dropped {before - after} rows with missing title")
    
    # For price column - drop rows with missing price (critical)
    if 'price' in df.columns:
        before = len(df)
        df = df.dropna(subset=['price'])
        after = len(df)
        if before > after:
            logger.info(f"Dropped {before - after} rows with missing price")
    
    # For rating column - fill missing with 'Unknown'
    if 'rating' in df.columns:
        before_missing = df['rating'].isnull().sum()
        if before_missing > 0:
            df['rating'] = df['rating'].fillna('Unknown')
            logger.info(f"Filled {before_missing} missing ratings with 'Unknown'")
    
    # For availability column - fill missing with 'Unknown'
    if 'availability' in df.columns:
        before_missing = df['availability'].isnull().sum()
        if before_missing > 0:
            df['availability'] = df['availability'].fillna('Unknown')
            logger.info(f"Filled {before_missing} missing availability with 'Unknown'")
    
    logger.info(f"Rows before fixing missing values: {original_count}, after: {len(df)}")
    
    return df


def clean_text_columns(df):
    """
    Clean text columns by stripping spaces and fixing special characters.
    
    This function:
    - Removes leading/trailing spaces
    - Fixes common encoding issues (â becomes ')
    - Removes extra whitespace inside text
    
    Parameters:
        df: pandas DataFrame to clean
    
    Returns:
        DataFrame with cleaned text
    """
    
    if df is None or df.empty:
        return df
    
    # List of common special character replacements
    replacements = {
        'â': "'",      # Smart apostrophe to straight apostrophe
        'â': '"',      # Smart opening quote to straight quote
        'â': '"',      # Smart closing quote to straight quote
        'â': '-',      # En dash to hyphen
        'â': '-',      # Em dash to hyphen
        'â¦': '...',    # Ellipsis
        '\n': ' ',       # Newline to space
        '\r': ' ',       # Carriage return to space
        '\t': ' ',       # Tab to space
    }
    
    # Columns that contain text
    text_columns = ['title', 'rating', 'availability']
    
    for col in text_columns:
        if col in df.columns:
            # Convert to string type first
            df[col] = df[col].astype(str)
            
            # Strip leading and trailing spaces
            df[col] = df[col].str.strip()
            
            # Replace special characters
            for old, new in replacements.items():
                df[col] = df[col].str.replace(old, new, regex=False)
            
            # Remove extra spaces (multiple spaces变成 single space)
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
            
            # Strip again after replacements
            df[col] = df[col].str.strip()
            
            logger.info(f"Cleaned text column: {col}")
    
    return df


def filter_invalid_prices(df, min_price=0, max_price=1000):
    """
    Remove rows with invalid prices (negative, zero, or too high).
    
    Parameters:
        df: pandas DataFrame to clean
        min_price: Minimum acceptable price
        max_price: Maximum acceptable price
    
    Returns:
        DataFrame with invalid prices removed
    """
    
    if df is None or df.empty:
        return df
    
    if 'price' not in df.columns:
        return df
    
    original_count = len(df)
    
    # Try to convert price to numeric, coerce errors to NaN
    # This handles cases where price is still text or has special characters
    df['price_numeric'] = pd.to_numeric(df['price'], errors='coerce')
    
    # Filter for valid prices
    valid_mask = (df['price_numeric'] >= min_price) & (df['price_numeric'] <= max_price)
    invalid_count = (~valid_mask).sum()
    
    if invalid_count > 0:
        df = df[valid_mask]
        logger.info(f"Removed {invalid_count} rows with invalid prices")
        logger.info(f"  - Price must be between ${min_price} and ${max_price}")
    
    # Drop the temporary numeric column
    df = df.drop(columns=['price_numeric'])
    
    logger.info(f"Rows before price filter: {original_count}, after: {len(df)}")
    
    return df


def remove_outliers_by_price(df, multiplier=3):
    """
    Remove price outliers using the IQR method.
    
    Outliers are prices that are too high compared to other books.
    This is optional but can improve data quality.
    
    Parameters:
        df: pandas DataFrame to clean
        multiplier: IQR multiplier (3 is standard, higher means less aggressive)
    
    Returns:
        DataFrame with outliers removed
    """
    
    if df is None or df.empty:
        return df
    
    if 'price' not in df.columns:
        return df
    
    original_count = len(df)
    
    # Convert price to numeric
    prices = pd.to_numeric(df['price'], errors='coerce')
    
    # Calculate Q1 (25th percentile) and Q3 (75th percentile)
    Q1 = prices.quantile(0.25)
    Q3 = prices.quantile(0.75)
    IQR = Q3 - Q1
    
    # Define bounds for outliers
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    
    # Filter out outliers
    valid_mask = (prices >= lower_bound) & (prices <= upper_bound)
    outlier_count = (~valid_mask).sum()
    
    if outlier_count > 0:
        df = df[valid_mask]
        logger.info(f"Removed {outlier_count} price outliers")
        logger.info(f"  Price range kept: ${lower_bound:.2f} to ${upper_bound:.2f}")
    
    logger.info(f"Rows before outlier removal: {original_count}, after: {len(df)}")
    
    return df


def run_full_cleaning(df):
    """
    Run all cleaning functions in the correct order.
    
    This is the main function that orchestrates all cleaning steps.
    
    Parameters:
        df: pandas DataFrame to clean
    
    Returns:
        Cleaned DataFrame
    """
    
    if df is None or df.empty:
        logger.warning("No data to clean")
        return df
    
    print('-'*140)
    print("CLEANING DATA")
    print('-'*140)
    
    # Step 1: Fix missing values
    print("Step 1: Fixing missing values...")
    df = fix_missing_values(df)
    
    # Step 2: Clean text columns (special characters, spaces)
    print("Step 2: Cleaning text columns...")
    df = clean_text_columns(df)
    
    # Step 3: Remove duplicates
    print("Step 3: Removing duplicates...")
    df = remove_duplicates(df, column='title')
    
    # Step 4: Filter invalid prices
    print("Step 4: Filtering invalid prices...")
    df = filter_invalid_prices(df, min_price=0, max_price=200)
    
    print('-'*140)
    print(f"CLEANING COMPLETE: Final row count = {len(df)}")
    print('-'*140)
    
    logger.info(f"Cleaning complete. Final row count: {len(df)}")
    
    return df


# This code runs only when you run this file directly
if __name__ == "__main__":
    print('-'*140)
    print("TESTING clean_data.py MODULE")
    print('-'*140)
    
    # Setup basic logging
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data for testing
    sample_data = pd.DataFrame([
        {'title': '  Test Book 1  ', 'price': '51.77', 'rating': 'Three', 'availability': 'In stock'},
        {'title': 'Test Book 2', 'price': '-5.00', 'rating': 'One', 'availability': 'In stock'},
        {'title': 'Test Book 1', 'price': '51.77', 'rating': 'Three', 'availability': 'In stock'},  # Duplicate
        {'title': None, 'price': '10.00', 'rating': 'Two', 'availability': 'In stock'},  # Missing title
        {'title': 'Test Book 3', 'price': '999.99', 'rating': 'Five', 'availability': 'In stock'},  # Outlier
        {'title': 'Test Book 4', 'price': '25.00', 'rating': 'Four', 'availability': None},  # Missing availability
    ])
    
    print(f"Sample data has {len(sample_data)} rows")
    print('-'*140)
    
    # Run cleaning
    cleaned_df = run_full_cleaning(sample_data)
    
    print("\nCLEANED DATA:")
    print(cleaned_df)
    print('-'*140)
    print("SUCCESS: clean_data.py is working correctly")
    print('-'*140)