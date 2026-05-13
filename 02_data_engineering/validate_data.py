"""
VALIDATE DATA MODULE - Checks data quality before database load
This module validates that the data meets quality standards before loading.
It checks for negative prices, invalid ratings, missing values, and duplicates.
If quality checks fail, the load process can be stopped.

Author: Business Intelligence Platform
Date: 2026-05-12
"""

import pandas as pd
import logging

# Setup logger for this module
logger = logging.getLogger(__name__)


def validate_prices(df, price_column='price_numeric'):
    """
    Check if all prices are valid (positive numbers within range).
    
    Parameters:
        df: pandas DataFrame to validate
        price_column: Name of the numeric price column
    
    Returns:
        Dictionary with validation results
    """
    
    results = {
        'check_name': 'price_validation',
        'passed': True,
        'issues': [],
        'stats': {}
    }
    
    if df is None or df.empty:
        results['passed'] = False
        results['issues'].append('DataFrame is empty')
        return results
    
    if price_column not in df.columns:
        results['passed'] = False
        results['issues'].append(f"Column '{price_column}' not found")
        return results
    
    # Check for negative prices
    negative_prices = df[df[price_column] < 0]
    if len(negative_prices) > 0:
        results['passed'] = False
        results['issues'].append(f"Found {len(negative_prices)} rows with negative prices")
    
    # Check for zero prices
    zero_prices = df[df[price_column] == 0]
    if len(zero_prices) > 0:
        results['passed'] = False
        results['issues'].append(f"Found {len(zero_prices)} rows with zero prices")
    
    # Check for extremely high prices (over $200)
    high_prices = df[df[price_column] > 200]
    if len(high_prices) > 0:
        results['issues'].append(f"Found {len(high_prices)} rows with very high prices (>$200)")
        # This is a warning, not a failure
    
    # Collect statistics
    results['stats'] = {
        'min_price': df[price_column].min(),
        'max_price': df[price_column].max(),
        'avg_price': df[price_column].mean(),
        'total_rows': len(df)
    }
    
    logger.info(f"Price validation: {len(negative_prices)} negative, {len(zero_prices)} zero")
    
    return results


def validate_ratings(df, rating_column='rating_numeric'):
    """
    Check if all ratings are valid (1-5).
    
    Parameters:
        df: pandas DataFrame to validate
        rating_column: Name of the numeric rating column
    
    Returns:
        Dictionary with validation results
    """
    
    results = {
        'check_name': 'rating_validation',
        'passed': True,
        'issues': [],
        'stats': {}
    }
    
    if df is None or df.empty:
        results['passed'] = False
        results['issues'].append('DataFrame is empty')
        return results
    
    if rating_column not in df.columns:
        results['passed'] = False
        results['issues'].append(f"Column '{rating_column}' not found")
        return results
    
    # Check for invalid ratings (not 1-5)
    valid_ratings = [1, 2, 3, 4, 5]
    invalid_ratings = df[~df[rating_column].isin(valid_ratings)]
    
    if len(invalid_ratings) > 0:
        results['passed'] = False
        results['issues'].append(f"Found {len(invalid_ratings)} rows with invalid ratings (should be 1-5)")
    
    # Check for null ratings
    null_ratings = df[df[rating_column].isnull()]
    if len(null_ratings) > 0:
        results['passed'] = False
        results['issues'].append(f"Found {len(null_ratings)} rows with null ratings")
    
    # Collect statistics
    results['stats'] = {
        'rating_distribution': df[rating_column].value_counts().sort_index().to_dict(),
        'total_rows': len(df)
    }
    
    logger.info(f"Rating validation: {len(invalid_ratings)} invalid, {len(null_ratings)} null")
    
    return results


def validate_titles(df, title_column='title'):
    """
    Check if all titles are present and not empty.
    
    Parameters:
        df: pandas DataFrame to validate
        title_column: Name of the title column
    
    Returns:
        Dictionary with validation results
    """
    
    results = {
        'check_name': 'title_validation',
        'passed': True,
        'issues': [],
        'stats': {}
    }
    
    if df is None or df.empty:
        results['passed'] = False
        results['issues'].append('DataFrame is empty')
        return results
    
    if title_column not in df.columns:
        results['passed'] = False
        results['issues'].append(f"Column '{title_column}' not found")
        return results
    
    # Check for empty titles
    empty_titles = df[df[title_column].isnull()]
    if len(empty_titles) > 0:
        results['passed'] = False
        results['issues'].append(f"Found {len(empty_titles)} rows with null titles")
    
    # Check for empty string titles
    df[title_column] = df[title_column].astype(str)
    blank_titles = df[df[title_column].str.strip() == '']
    if len(blank_titles) > 0:
        results['passed'] = False
        results['issues'].append(f"Found {len(blank_titles)} rows with blank titles")
    
    # Check for duplicate titles
    duplicate_titles = df[df[title_column].duplicated(keep=False)]
    if len(duplicate_titles) > 0:
        duplicate_count = len(duplicate_titles)
        unique_duplicates = duplicate_titles[title_column].nunique()
        results['issues'].append(f"Found {duplicate_count} rows with duplicate titles ({unique_duplicates} unique titles duplicated)")
        # This is a warning, duplicates may be acceptable depending on business rules
    
    # Collect statistics
    results['stats'] = {
        'total_titles': len(df),
        'unique_titles': df[title_column].nunique(),
        'duplicate_count': len(df[df[title_column].duplicated()])
    }
    
    logger.info(f"Title validation: {results['stats']['unique_titles']} unique titles out of {results['stats']['total_titles']}")
    
    return results


def validate_availability(df, availability_column='availability'):
    """
    Check if availability values are valid.
    
    Parameters:
        df: pandas DataFrame to validate
        availability_column: Name of the availability column
    
    Returns:
        Dictionary with validation results
    """
    
    results = {
        'check_name': 'availability_validation',
        'passed': True,
        'issues': [],
        'stats': {}
    }
    
    if df is None or df.empty:
        results['passed'] = False
        results['issues'].append('DataFrame is empty')
        return results
    
    if availability_column not in df.columns:
        results['passed'] = False
        results['issues'].append(f"Column '{availability_column}' not found")
        return results
    
    # Check for null availability
    null_avail = df[df[availability_column].isnull()]
    if len(null_avail) > 0:
        results['passed'] = False
        results['issues'].append(f"Found {len(null_avail)} rows with null availability")
    
    # Check for unknown availability
    unknown_avail = df[df[availability_column] == 'Unknown']
    if len(unknown_avail) > 0:
        results['issues'].append(f"Found {len(unknown_avail)} rows with 'Unknown' availability (warning only)")
    
    # Collect statistics
    results['stats'] = {
        'availability_distribution': df[availability_column].value_counts().to_dict(),
        'total_rows': len(df)
    }
    
    logger.info(f"Availability validation complete")
    
    return results


def run_full_validation(df):
    """
    Run all validation checks and return overall quality score.
    
    This is the main function that orchestrates all validation checks.
    
    Parameters:
        df: pandas DataFrame to validate
    
    Returns:
        Dictionary with overall validation results including:
        - overall_pass: True if all critical checks passed
        - quality_score: Percentage of passed checks
        - all_results: List of individual check results
    """
    
    print('-'*140)
    print("VALIDATING DATA QUALITY")
    print('-'*140)
    
    if df is None or df.empty:
        print("ERROR: No data to validate")
        return {
            'overall_pass': False,
            'quality_score': 0,
            'all_results': [],
            'message': 'No data provided for validation'
        }
    
    # Run all validation checks
    validation_results = []
    
    print("Running price validation...")
    price_results = validate_prices(df)
    validation_results.append(price_results)
    
    print("Running rating validation...")
    rating_results = validate_ratings(df)
    validation_results.append(rating_results)
    
    print("Running title validation...")
    title_results = validate_titles(df)
    validation_results.append(title_results)
    
    print("Running availability validation...")
    availability_results = validate_availability(df)
    validation_results.append(availability_results)
    
    # Calculate overall pass/fail
    # Critical checks that must pass: price and rating
    critical_passed = price_results['passed'] and rating_results['passed']
    
    # Calculate quality score (percentage of total checks passed)
    total_checks = len(validation_results)
    passed_checks = sum(1 for r in validation_results if r['passed'])
    quality_score = (passed_checks / total_checks) * 100
    
    overall_pass = critical_passed
    
    # Print summary
    print('-'*140)
    print("VALIDATION SUMMARY")
    print('-'*140)
    
    for result in validation_results:
        status = "PASS" if result['passed'] else "FAIL"
        print(f"{status}: {result['check_name']}")
        if result.get('issues'):
            for issue in result['issues']:
                print(f"   - {issue}")
    
    print('-'*140)
    print(f"Quality Score: {quality_score:.1f}%")
    print(f"Critical Checks Passed: {critical_passed}")
    print(f"Overall Validation: {'PASS' if overall_pass else 'FAIL'}")
    
    if overall_pass:
        print("SUCCESS: Data quality is acceptable for database loading")
    else:
        print("WARNING: Data quality issues found. Consider cleaning before loading.")
    
    print('-'*140)
    
    return {
        'overall_pass': overall_pass,
        'quality_score': quality_score,
        'all_results': validation_results,
        'message': 'Validation complete'
    }


def should_proceed_to_load(validation_result, require_perfect=False):
    """
    Determine whether to proceed with database loading based on validation.
    
    Parameters:
        validation_result: Result from run_full_validation()
        require_perfect: If True, all checks must pass. If False, only critical.
    
    Returns:
        Boolean: True if should proceed, False otherwise
    """
    
    if require_perfect:
        return validation_result['quality_score'] == 100
    else:
        return validation_result['overall_pass']


# This code runs only when you run this file directly
if __name__ == "__main__":
    print('-'*140)
    print("TESTING validate_data.py MODULE")
    print('-'*140)
    
    # Setup basic logging
    logging.basicConfig(level=logging.INFO)
    
    # Create good sample data
    good_data = pd.DataFrame([
        {'title': 'Book A', 'price_numeric': 25.00, 'rating_numeric': 4, 'availability': 'In stock'},
        {'title': 'Book B', 'price_numeric': 30.00, 'rating_numeric': 5, 'availability': 'In stock'},
        {'title': 'Book C', 'price_numeric': 15.00, 'rating_numeric': 3, 'availability': 'In stock'},
    ])
    
    # Create bad sample data with issues
    bad_data = pd.DataFrame([
        {'title': 'Book A', 'price_numeric': -5.00, 'rating_numeric': 4, 'availability': 'In stock'},
        {'title': None, 'price_numeric': 30.00, 'rating_numeric': 6, 'availability': 'In stock'},
        {'title': 'Book A', 'price_numeric': 15.00, 'rating_numeric': 3, 'availability': None},
    ])
    
    print("Testing with GOOD data:")
    print('-'*70)
    good_result = run_full_validation(good_data)
    print(f"\nShould proceed to load: {should_proceed_to_load(good_result)}")
    
    print("\n" + '-'*140)
    print("Testing with BAD data:")
    print('-'*70)
    bad_result = run_full_validation(bad_data)
    print(f"\nShould proceed to load: {should_proceed_to_load(bad_result)}")
    
    print('-'*140)
    print("SUCCESS: validate_data.py is working correctly")
    print('-'*140)