"""
CHARTS - All visualization functions for the dashboard
"""

import streamlit as st
import plotly.express as px
import pandas as pd


def display_metrics(df):
    """Show key metrics in big number cards."""
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_books = len(df)
        st.metric("Total Books", total_books)
    
    with col2:
        avg_price = df['price_numeric'].mean()
        st.metric("Average Price", f"${avg_price:.2f}")
    
    with col3:
        min_price = df['price_numeric'].min()
        max_price = df['price_numeric'].max()
        st.metric("Price Range", f"${min_price:.2f} - ${max_price:.2f}")
    
    with col4:
        unique_ratings = df['rating_name'].nunique()
        st.metric("Rating Types", f"{unique_ratings} (1-5 stars)")


def display_price_distribution(df):
    """Show histogram of book prices."""
    
    st.subheader("Price Distribution")
    
    fig = px.histogram(
        df, 
        x='price_numeric',
        nbins=20,
        title="Distribution of Book Prices",
        labels={'price_numeric': 'Price ($)', 'count': 'Number of Books'},
        color_discrete_sequence=['#1f77b4']
    )
    
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    with st.expander("How to interpret this chart"):
        st.markdown("""
        **What this chart shows:**
        - Each bar shows how many books fall into a specific price range
        - Taller bars mean more books at that price point
        
        **What to look for:**
        - Left-heavy: Mostly budget books
        - Right-heavy: Mostly premium books
        - Two peaks: Mix of budget and premium
        """)


def display_rating_distribution(df):
    """Show bar chart of book ratings."""
    
    st.subheader("Rating Distribution")
    
    rating_counts = df['rating_name'].value_counts().reset_index()
    rating_counts.columns = ['Rating', 'Count']
    
    rating_order = ['One', 'Two', 'Three', 'Four', 'Five']
    rating_counts['Rating'] = pd.Categorical(rating_counts['Rating'], categories=rating_order, ordered=True)
    rating_counts = rating_counts.sort_values('Rating')
    
    fig = px.bar(
        rating_counts,
        x='Rating',
        y='Count',
        title="Number of Books by Rating",
        labels={'Rating': 'Star Rating', 'Count': 'Number of Books'},
        color='Count',
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    with st.expander("How to interpret this chart"):
        st.markdown("""
        **What this chart shows:**
        - Each bar shows how many books have 1, 2, 3, 4, or 5 stars
        
        **What to look for:**
        - More 4-5 stars: Customers are satisfied
        - More 1-2 stars: Quality issues may exist
        """)


def display_price_by_rating(df):
    """Show average price for each rating level."""
    
    st.subheader("Average Price by Rating")
    
    price_by_rating = df.groupby('rating_name')['price_numeric'].mean().reset_index()
    price_by_rating.columns = ['Rating', 'Average Price']
    
    rating_order = ['One', 'Two', 'Three', 'Four', 'Five']
    price_by_rating['Rating'] = pd.Categorical(price_by_rating['Rating'], categories=rating_order, ordered=True)
    price_by_rating = price_by_rating.sort_values('Rating')
    
    fig = px.line(
        price_by_rating,
        x='Rating',
        y='Average Price',
        title="Average Price by Star Rating",
        labels={'Rating': 'Star Rating', 'Average Price': 'Average Price ($)'},
        markers=True
    )
    
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    with st.expander("How to interpret this chart"):
        st.markdown("""
        **What this chart shows:**
        - Upward line: Higher rated books cost more
        - Downward line: Higher rated books cost less
        - Flat line: Rating does not affect price
        """)


def display_price_categories(df):
    """Show pie chart of budget, mid, and premium books."""
    
    st.subheader("Books by Price Category")
    
    category_counts = df['price_category'].value_counts().reset_index()
    category_counts.columns = ['Category', 'Count']
    
    fig = px.pie(
        category_counts,
        values='Count',
        names='Category',
        title="Percentage of Books by Price Category",
        color='Category',
        color_discrete_map={
            'Budget': '#2ecc71',
            'Mid': '#f39c12',
            'Premium': '#e74c3c'
        }
    )
    
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    with st.expander("How to interpret this chart"):
        st.markdown("""
        **What this chart shows:**
        - Budget: Books under $20
        - Mid: Books $20-$40
        - Premium: Books over $40
        """)


def display_top_expensive(df):
    """Show the 10 most expensive books."""
    
    st.subheader("Top 10 Most Expensive Books")
    
    top_expensive = df.nlargest(10, 'price_numeric')[['book_title', 'price_numeric', 'rating_name']]
    top_expensive.columns = ['Book Title', 'Price ($)', 'Rating']
    
    st.dataframe(top_expensive, use_container_width=True, hide_index=True)


def display_best_value(df):
    """Show high-rated books with low prices."""
    
    st.subheader("Best Value Books (High Rating, Low Price)")
    
    high_rated = df[df['rating_number'] >= 4]
    best_value = high_rated.nsmallest(10, 'price_numeric')[['book_title', 'price_numeric', 'rating_name']]
    best_value.columns = ['Book Title', 'Price ($)', 'Rating']
    
    if len(best_value) > 0:
        st.dataframe(best_value, use_container_width=True, hide_index=True)
    else:
        st.info("No high-rated books found in the data.")