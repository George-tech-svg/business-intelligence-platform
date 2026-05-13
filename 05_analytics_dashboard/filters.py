"""
FILTERS - Sidebar filter functions
"""

import streamlit as st


def display_filters(df):
    """
    Show filters in the sidebar and return filtered dataframe.
    """
    
    st.sidebar.header("Filters")
    
    # Rating filter
    st.sidebar.subheader("Filter by Rating")
    selected_ratings = st.sidebar.multiselect(
        "Select ratings to include:",
        options=df['rating_name'].unique(),
        default=df['rating_name'].unique()
    )
    
    # Price category filter
    st.sidebar.subheader("Filter by Price Category")
    selected_categories = st.sidebar.multiselect(
        "Select price categories:",
        options=df['price_category'].unique(),
        default=df['price_category'].unique()
    )
    
    # Price range filter
    st.sidebar.subheader("Filter by Price Range")
    min_price = float(df['price_numeric'].min())
    max_price = float(df['price_numeric'].max())
    
    price_range = st.sidebar.slider(
        "Price range ($):",
        min_value=min_price,
        max_value=max_price,
        value=(min_price, max_price)
    )
    
    # Apply filters
    filtered_df = df[
        (df['rating_name'].isin(selected_ratings)) &
        (df['price_category'].isin(selected_categories)) &
        (df['price_numeric'] >= price_range[0]) &
        (df['price_numeric'] <= price_range[1])
    ]
    
    # Show filter summary
    st.sidebar.markdown("---")
    st.sidebar.metric("Filtered Books", len(filtered_df))
    st.sidebar.metric("Original Books", len(df))
    
    return filtered_df