"""
DATA LOADER - Loads data from MySQL database
"""

import mysql.connector
import pandas as pd
import streamlit as st
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LOCAL_DB_CONFIG, ACTIVE_DB


@st.cache_data(ttl=3600)
def load_data():
    """
    Load book data from MySQL database.
    Cached to avoid reloading on every interaction.
    """
    
    if ACTIVE_DB == 'local':
        db_config = LOCAL_DB_CONFIG
    else:
        st.error("Cloud database not configured")
        return None
    
    try:
        connection = mysql.connector.connect(**db_config)
        
        query = """
        SELECT 
            db.book_title,
            f.price_numeric,
            dr.rating_name,
            dr.rating_number,
            dr.rating_category,
            dpc.category_name as price_category,
            dd.full_date as scraped_date
        FROM fact_book_analysis f
        INNER JOIN dim_book db ON f.book_id = db.book_id
        INNER JOIN dim_rating dr ON f.rating_id = dr.rating_id
        INNER JOIN dim_date dd ON f.date_id = dd.date_id
        INNER JOIN dim_price_category dpc ON f.category_id = dpc.category_id
        ORDER BY f.price_numeric DESC
        """
        
        df = pd.read_sql(query, connection)
        connection.close()
        return df
        
    except Exception as error:
        st.error(f"Error loading data: {error}")
        return None