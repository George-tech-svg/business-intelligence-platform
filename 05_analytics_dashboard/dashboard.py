"""
MAIN DASHBOARD FILE - Business Intelligence Dashboard
Run this file to start the dashboard: streamlit run dashboard.py
"""

import streamlit as st
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

# Import our custom modules
from data_loader import load_data
from charts import (
    display_metrics,
    display_price_distribution,
    display_rating_distribution,
    display_price_by_rating,
    display_price_categories,
    display_top_expensive,
    display_best_value
)
from filters import display_filters

# Page configuration
st.set_page_config(
    page_title="Book Analytics Dashboard",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

print('-'*140)
print("PHASE 5: ANALYTICS DASHBOARD - STARTING")
print('-'*140)


def main():
    """
    Main function to run the dashboard.
    """
    
    # Dashboard title
    st.title("Book Analytics Dashboard")
    st.markdown("### Business Intelligence Platform")
    st.markdown("Real-time insights from your book data")
    st.markdown("---")
    
    # Load data
    with st.spinner("Loading data from database..."):
        df = load_data()
    
    if df is None or df.empty:
        st.error("No data found. Please run the ETL pipeline first.")
        return
    
    # Display filters in sidebar
    filtered_df = display_filters(df)
    
    # Display key metrics
    display_metrics(filtered_df)
    
    st.markdown("---")
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        display_price_distribution(filtered_df)
    
    with col2:
        display_rating_distribution(filtered_df)
    
    # Full width charts
    display_price_by_rating(filtered_df)
    
    col3, col4 = st.columns(2)
    
    with col3:
        display_price_categories(filtered_df)
    
    with col4:
        st.subheader("Quick Stats")
        
        # FIXED: Safe way to get most common rating
        rating_counts = filtered_df['rating_name'].value_counts()
        if len(rating_counts) > 0:
            most_common_rating = rating_counts.index[0]
        else:
            most_common_rating = "No data"
        
        # FIXED: Safe way to get most common price category
        category_counts = filtered_df['price_category'].value_counts()
        if len(category_counts) > 0:
            most_common_category = category_counts.index[0]
        else:
            most_common_category = "No data"
        
        st.markdown(f"""
        - **Most common rating:** {most_common_rating}
        - **Most common price category:** {most_common_category}
        - **Average rating:** {filtered_df['rating_number'].mean():.1f} stars
        """)
    
    st.markdown("---")
    
    # Book lists
    col5, col6 = st.columns(2)
    
    with col5:
        display_top_expensive(filtered_df)
    
    with col6:
        display_best_value(filtered_df)
    
    # Footer
    st.markdown("---")
    st.caption("Data source: Books to Scrape | Dashboard built with Streamlit")


if __name__ == "__main__":
    main()