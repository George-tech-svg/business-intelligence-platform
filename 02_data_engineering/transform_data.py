"""
TRANSFORM DATA MODULE - Converts data to correct formats
This module transforms raw data into the right formats for database storage.
It converts text ratings to numbers, text prices to decimals, and adds calculated columns.

Author: Business Intelligence Platform
Date: 2026-05-12
"""

import pandas as pd
import logging

# Setup logger for this module
logger = logging.getLogger(__name__)


def convert_rating_to_number(df, rating_column='rating'):
    """
    Convert rating text to numeric values.
    
    Books to Scrape uses words for ratings:
    'One' = 1 star, 'Two' = 2 stars, 'Three' = 3 stars, etc.
    
    Parameters:
        df: pandas DataFrame
        rating_column: Name of the column containing ratings
    
    Returns:
        DataFrame with new numeric rating column
    """
    
    if df is None or df.empty:
        return df
    
    if rating_column not in df.columns:
        logger.warning(f"Column '{rating_column}' not found")
        return df
    
    # Create mapping from text to number
    rating_map = {
        'One': 1,
        'Two': 2,
        'Three': 3,
        'Four': 4,
        'Five': 5,
        'Unknown': 0
    }
    
    # Apply the mapping to create a new column
    df['rating_numeric'] = df[rating_column].map(rating_map)
    
    # Check for any unmapped values
    unmapped = df[df['rating_numeric'].isnull()]
    if len(unmapped) > 0:
        logger.warning(f"Found {len(unmapped)} rows with unmapped ratings")
        # Set unmapped to 0
        df['rating_numeric'] = df['rating_numeric'].fillna(0)
    
    # Convert to integer type (no decimal points)
    df['rating_numeric'] = df['rating_numeric'].astype(int)
    
    logger.info(f"Converted ratings: {df['rating_numeric'].value_counts().sort_index().to_dict()}")
    
    return df


def convert_price_to_number(df, price_column='price'):
    """
    Convert price text to numeric decimal values.
    
    Prices come as strings like '51.77' or 'Â£51.77'
    This function extracts just the number and converts to float.
    
    Parameters:
        df: pandas DataFrame
        price_column: Name of the column containing prices
    
    Returns:
        DataFrame with new numeric price column
    """
    
    if df is None or df.empty:
        return df
    
    if price_column not in df.columns:
        logger.warning(f"Column '{price_column}' not found")
        return df
    
    # Remove currency symbols and convert to number
    # First, ensure column is string type
    df[price_column] = df[price_column].astype(str)
    
    # Remove £ symbol and any other non-numeric characters except dot
    df['price_numeric'] = df[price_column].str.replace('£', '', regex=False)
    df['price_numeric'] = df['price_numeric'].str.replace(',', '', regex=False)
    df['price_numeric'] = df['price_numeric'].str.replace('Â', '', regex=False)
    
    # Remove anything that is not a number or dot
    df['price_numeric'] = df['price_numeric'].str.extract(r'(\d+\.?\d*)')[0]
    
    # Convert to float (decimal number)
    df['price_numeric'] = pd.to_numeric(df['price_numeric'], errors='coerce')
    
    # Count invalid prices
    invalid_count = df['price_numeric'].isnull().sum()
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} rows with invalid prices")
    
    logger.info(f"Price range: ${df['price_numeric'].min():.2f} to ${df['price_numeric'].max():.2f}")
    logger.info(f"Average price: ${df['price_numeric'].mean():.2f}")
    
    return df


def add_price_category(df, price_column='price_numeric'):
    """
    Add a price category column for analysis.
    
    Categories:
    - Budget: price < $20
    - Mid: $20 <= price < $40
    - Premium: price >= $40
    
    Parameters:
        df: pandas DataFrame
        price_column: Name of the numeric price column
    
    Returns:
        DataFrame with new price_category column
    """
    
    if df is None or df.empty:
        return df
    
    if price_column not in df.columns:
        logger.warning(f"Column '{price_column}' not found")
        return df
    
    # Define category based on price
    def categorize_price(price):
        if pd.isna(price):
            return 'Unknown'
        elif price < 20:
            return 'Budget'
        elif price < 40:
            return 'Mid'
        else:
            return 'Premium'
    
    df['price_category'] = df[price_column].apply(categorize_price)
    
    # Log distribution
    category_counts = df['price_category'].value_counts()
    logger.info(f"Price category distribution: {category_counts.to_dict()}")
    
    return df


def add_rating_category(df, rating_column='rating_numeric'):
    """
    Add a rating category column for analysis.
    
    Categories:
    - Low: 1-2 stars
    - Medium: 3 stars
    - High: 4-5 stars
    
    Parameters:
        df: pandas DataFrame
        rating_column: Name of the numeric rating column
    
    Returns:
        DataFrame with new rating_category column
    """
    
    if df is None or df.empty:
        return df
    
    if rating_column not in df.columns:
        logger.warning(f"Column '{rating_column}' not found")
        return df
    
    # Define category based on rating
    def categorize_rating(rating):
        if pd.isna(rating) or rating == 0:
            return 'Unknown'
        elif rating <= 2:
            return 'Low'
        elif rating == 3:
            return 'Medium'
        else:
            return 'High'
    
    df['rating_category'] = df[rating_column].apply(categorize_rating)
    
    # Log distribution
    category_counts = df['rating_category'].value_counts()
    logger.info(f"Rating category distribution: {category_counts.to_dict()}")
    
    return df


def standardize_date_format(df, date_column='scraped_at'):
    """
    Standardize date format to a consistent format.
    
    Parameters:
        df: pandas DataFrame
        date_column: Name of the column containing dates
    
    Returns:
        DataFrame with standardized date column
    """
    
    if df is None or df.empty:
        return df
    
    if date_column not in df.columns:
        logger.warning(f"Column '{date_column}' not found")
        return df
    
    # Convert to datetime object
    df['scraped_date'] = pd.to_datetime(df[date_column], errors='coerce')
    
    # Extract useful date parts
    df['scraped_year'] = df['scraped_date'].dt.year
    df['scraped_month'] = df['scraped_date'].dt.month
    df['scraped_day'] = df['scraped_date'].dt.day
    df['scraped_hour'] = df['scraped_date'].dt.hour
    
    logger.info(f"Date range: {df['scraped_date'].min()} to {df['scraped_date'].max()}")
    
    return df


def clean_title_for_search(df, title_column='title'):
    """
    Create a cleaned title column for better searching.
    
    This removes punctuation and converts to lowercase for easier matching.
    
    Parameters:
        df: pandas DataFrame
        title_column: Name of the title column
    
    Returns:
        DataFrame with new searchable_title column
    """
    
    if df is None or df.empty:
        return df
    
    if title_column not in df.columns:
        logger.warning(f"Column '{title_column}' not found")
        return df
    
    # Convert to lowercase
    df['searchable_title'] = df[title_column].str.lower()
    
    # Remove punctuation
    df['searchable_title'] = df['searchable_title'].str.replace(r'[^\w\s]', '', regex=True)
    
    # Remove extra spaces
    df['searchable_title'] = df['searchable_title'].str.replace(r'\s+', ' ', regex=True)
    
    # Strip leading/trailing spaces
    df['searchable_title'] = df['searchable_title'].str.strip()
    
    logger.info("Created searchable_title column for better text matching")
    
    return df


def run_full_transformation(df):
    """
    Run all transformation functions in the correct order.
    
    This is the main function that orchestrates all transformations.
    
    Parameters:
        df: pandas DataFrame to transform
    
    Returns:
        Transformed DataFrame
    """
    
    if df is None or df.empty:
        logger.warning("No data to transform")
        return df
    
    print('-'*140)
    print("TRANSFORMING DATA")
    print('-'*140)
    
    # Step 1: Convert rating text to number
    print("Step 1: Converting rating text to numbers...")
    df = convert_rating_to_number(df)
    
    # Step 2: Convert price text to numbers
    print("Step 2: Converting price text to numbers...")
    df = convert_price_to_number(df)
    
    # Step 3: Add price category column
    print("Step 3: Adding price category...")
    df = add_price_category(df)
    
    # Step 4: Add rating category column
    print("Step 4: Adding rating category...")
    df = add_rating_category(df)
    
    # Step 5: Standardize date format
    print("Step 5: Standardizing date format...")
    df = standardize_date_format(df)
    
    # Step 6: Create searchable title
    print("Step 6: Creating searchable title...")
    df = clean_title_for_search(df)
    
    print('-'*140)
    print("TRANSFORMATION COMPLETE")
    print(f"Final columns: {list(df.columns)}")
    print('-'*140)
    
    logger.info("Transformation complete")
    
    return df


# This code runs only when you run this file directly
if __name__ == "__main__":
    print('-'*140)
    print("TESTING transform_data.py MODULE")
    print('-'*140)
    
    # Setup basic logging
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data for testing
    sample_data = pd.DataFrame([
        {'title': 'A Light in the Attic', 'price': '51.77', 'rating': 'Three', 'scraped_at': '2026-05-12 14:58:01'},
        {'title': 'Tipping the Velvet', 'price': '53.74', 'rating': 'One', 'scraped_at': '2026-05-12 14:58:01'},
        {'title': 'Soumission', 'price': '50.10', 'rating': 'One', 'scraped_at': '2026-05-12 14:58:01'},
        {'title': 'Sharp Objects', 'price': '47.82', 'rating': 'Four', 'scraped_at': '2026-05-12 14:58:01'},
        {'title': 'Sapiens', 'price': '54.23', 'rating': 'Five', 'scraped_at': '2026-05-12 14:58:01'},
    ])
    
    print(f"Sample data has {len(sample_data)} rows")
    print('-'*140)
    
    # Run transformation
    transformed_df = run_full_transformation(sample_data)
    
    print("\nTRANSFORMED DATA (selected columns):")
    print('-'*140)
    display_cols = ['title', 'price', 'price_numeric', 'rating', 'rating_numeric', 'price_category', 'rating_category']
    available_cols = [col for col in display_cols if col in transformed_df.columns]
    print(transformed_df[available_cols])
    print('-'*140)
    
    print("SUCCESS: transform_data.py is working correctly")
    print('-'*140)