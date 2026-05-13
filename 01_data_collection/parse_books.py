"""
PARSE BOOKS MODULE - Extracts book data from HTML
This module takes raw HTML from a webpage and finds all the book information.
It looks for titles, prices, ratings, and availability.

Author: Business Intelligence Platform
Date: 2026-05-12
"""

from bs4 import BeautifulSoup
import logging
from datetime import datetime

# Setup logger for this module
logger = logging.getLogger(__name__)


def parse_book_page(html):
    """
    Extract all book information from a single page HTML.
    
    This function takes the HTML from a books listing page and
    finds every book on that page. It extracts title, price,
    rating, and availability for each book.
    
    Parameters:
        html: The HTML text from the webpage (string)
    
    Returns:
        List of dictionaries. Each dictionary contains one book's data.
        Example: [
            {'title': 'A Light in the Attic', 'price': '51.77', 'rating': 'Three', 'availability': 'In stock'},
            {'title': 'Tipping the Velvet', 'price': '53.74', 'rating': 'One', 'availability': 'In stock'}
        ]
        Returns empty list if no books found.
    """
    
    books_data = []
    
    # Check if we have HTML to parse
    if not html:
        logger.warning("No HTML provided to parse")
        return books_data
    
    # Create BeautifulSoup object to parse the HTML
    # 'html.parser' tells BeautifulSoup to read HTML format
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find all book containers on the page
    # Each book is inside an <article> tag with class "product_pod"
    # This is specific to the Books to Scrape website
    book_articles = soup.find_all('article', class_='product_pod')
    
    logger.info(f"Found {len(book_articles)} books on this page")
    
    # Loop through each book and extract its information
    for book in book_articles:
        try:
            # Extract Book Title
            # Title is inside an <h3> tag, then an <a> tag
            # The title is stored in the 'title' attribute of the <a> tag
            title_element = book.find('h3').find('a')
            if title_element:
                title = title_element.get('title', 'No title found')
            else:
                title = 'Title not found'
            
            # Extract Price
            # Price is inside a <p> tag with class "price_color"
            price_element = book.find('p', class_='price_color')
            if price_element:
                # Clean the price text
                # Original looks like: "£51.77" or "Â£51.77"
                price = price_element.text
                # Remove the pound sign and any weird characters
                price = price.replace('£', '').replace('Â', '').strip()
            else:
                price = 'Price not found'
            
            # Extract Rating
            # Rating is inside a <p> tag with class "star-rating X"
            # X can be: One, Two, Three, Four, Five
            rating_element = book.find('p', class_='star-rating')
            if rating_element:
                # Get all CSS classes of the element
                rating_classes = rating_element.get('class', [])
                # The rating is the second class (index 1)
                # Example: ['star-rating', 'Three'] -> rating = 'Three'
                if len(rating_classes) > 1:
                    rating = rating_classes[1]
                else:
                    rating = 'Rating not found'
            else:
                rating = 'Rating element not found'
            
            # Extract Availability
            # Availability is inside a <p> tag with class "instock availability"
            availability_element = book.find('p', class_='instock availability')
            if availability_element:
                # Get text and remove extra spaces
                availability = availability_element.text.strip()
                # If text is empty, try getting just the class indicator
                if not availability:
                    availability = 'In stock'  # Default for this site
            else:
                availability = 'Availability not specified'
            
            # Create a dictionary with all the book information
            book_info = {
                'title': title,
                'price': price,
                'rating': rating,
                'availability': availability,
                'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            books_data.append(book_info)
            logger.debug(f"Extracted book: {title} - {price} - {rating} stars")
            
        except Exception as error:
            # If one book fails to parse, log error and continue with next book
            logger.error(f"Error parsing a book: {error}")
            continue
    
    logger.info(f"Successfully parsed {len(books_data)} books from this page")
    return books_data


def parse_rating_to_number(rating_text):
    """
    Convert text rating to number of stars.
    
    Books to Scrape uses words: One, Two, Three, Four, Five
    This function converts those words to numbers 1, 2, 3, 4, 5
    
    Parameters:
        rating_text: String like 'One', 'Two', 'Three', 'Four', 'Five'
    
    Returns:
        Integer from 1 to 5, or 0 if rating not recognized
    """
    
    rating_map = {
        'One': 1,
        'Two': 2,
        'Three': 3,
        'Four': 4,
        'Five': 5
    }
    
    return rating_map.get(rating_text, 0)


def get_total_books_from_pagination(html):
    """
    Find total number of books on the website from the pagination info.
    
    This is useful to know how many books we need to scrape.
    
    Parameters:
        html: HTML from the first page
    
    Returns:
        Total number of books as integer, or 0 if not found
    """
    
    if not html:
        return 0
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find the pagination section
    # It usually says something like "Page 1 of 50"
    form_element = soup.find('form', class_='form-horizontal')
    
    if form_element:
        # Look for strong tags which might contain the total
        strong_tags = form_element.find_all('strong')
        for tag in strong_tags:
            try:
                # Try to convert text to number
                total = int(tag.text)
                logger.info(f"Found total books: {total}")
                return total
            except ValueError:
                continue
    
    # Alternative: count how many books are listed as "Showing 1 to 20 of 1000"
    # This is a fallback method
    page_info = soup.find('li', class_='current')
    if page_info:
        text = page_info.text
        # Look for pattern like "of 1000"
        if 'of' in text:
            parts = text.split('of')
            if len(parts) > 1:
                try:
                    total = int(parts[1].strip())
                    logger.info(f"Found total books from pagination: {total}")
                    return total
                except ValueError:
                    pass
    
    logger.warning("Could not determine total number of books")
    return 0


# This code runs only when you run this file directly
# It is for testing purposes
if __name__ == "__main__":
    print('-'*140)
    print("TESTING parse_books.py MODULE")
    print('-'*140)
    
    # Setup basic logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Sample HTML for testing (simplified)
    sample_html = """
    <html>
        <body>
            <article class="product_pod">
                <h3><a title="Test Book 1">Test Book 1</a></h3>
                <p class="price_color">£51.77</p>
                <p class="star-rating Three"></p>
                <p class="instock availability">In stock</p>
            </article>
            <article class="product_pod">
                <h3><a title="Test Book 2">Test Book 2</a></h3>
                <p class="price_color">£53.74</p>
                <p class="star-rating One"></p>
                <p class="instock availability">In stock</p>
            </article>
        </body>
    </html>
    """
    
    print("Testing parse_book_page with sample HTML")
    print('-'*140)
    
    result = parse_book_page(sample_html)
    
    if result:
        print(f"SUCCESS: Parsed {len(result)} books")
        for book in result:
            print(f"  Title: {book['title']}")
            print(f"  Price: ${book['price']}")
            print(f"  Rating: {book['rating']} stars")
            print(f"  Available: {book['availability']}")
            print('-'*70)
    else:
        print("FAILED: Could not parse any books")
    
    print('-'*140)