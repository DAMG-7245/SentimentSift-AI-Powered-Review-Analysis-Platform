import streamlit as st
import requests
import pandas as pd
import json
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from PIL import Image
import io
import base64
import re

st.set_page_config(
    page_title="SentimentSift Restaurant Analysis",
    page_icon="🍽️",
    layout="wide"
)

# Custom CSS for better appearance
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #FF5A5F;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #484848;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #f7f7f7;
        border-radius: 5px;
        padding: 1rem;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    .review-card {
        background-color: #f9f9f9;
        border-left: 3px solid #FF5A5F;
        padding: 1rem;
        margin-bottom: 0.5rem;
        border-radius: 0 5px 5px 0;
    }
    .footer {
        margin-top: 3rem;
        text-align: center;
        color: #888;
    }
</style>
""", unsafe_allow_html=True)

# API endpoint configuration
API_URL = "http://localhost:8000"

# Header with logo
col1, col2 = st.columns([1, 5])
with col1:
    try:
        # Try to load the logo if available
        logo = Image.open("img/sentimentsift.png")
        st.image(logo, width=100)
    except:
        # If logo not found, display an emoji instead
        st.markdown("# 🍽️")
        
with col2:
    st.markdown('<div class="main-header">SentimentSift</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">AI-Powered Restaurant Review Analysis</div>', unsafe_allow_html=True)

# Create tabs for different functionality
tab1, tab2, tab3, tab4 = st.tabs(["Restaurant Search", "Analytics Dashboard", "City Explorer", "About"])

with tab1:
    st.markdown("### Search for Restaurants with Natural Language")
    
    # Input for natural language query with examples
    example_queries = [
        "Find restaurants in Boston with at least 4 stars",
        "What are the best cafe places in Boston?",
        "Show me top 5 restaurants in Boston with good service",
        "Where can I find romantic cafes in Boston?",
        "List family-friendly restaurants in Boston with 4.5+ stars"
    ]
    
    selected_example = st.selectbox("Try one of our example queries:", 
                                   [""] + example_queries)
    
    if selected_example:
        query = selected_example
    else:
        query = st.text_input("Or type your own query:", "Find cafes in Boston with good food")
    
    # Parameters and options
    col1, col2 = st.columns(2)
    with col1:
        show_sql = st.checkbox("Show SQL Query", value=True)
        show_map = st.checkbox("Show Map View", value=True)
    with col2:
        result_limit = st.slider("Result Limit", min_value=5, max_value=50, value=10)
        endpoint = st.radio("API Endpoint", ["Natural Language (query)", "Filtered Search (search)"])
    
    # Execute search
    if st.button("Find Restaurants", type="primary", key="search_button"):
        with st.spinner("Processing your query..."):
            try:
                # Determine which endpoint to use based on selection
                if endpoint == "Natural Language (query)":
                    response = requests.post(
                        f"{API_URL}/query",
                        json={"question": query}
                    )
                else:  # Filtered Search
                    # Extract parameters from query
                    city = "Boston"  # Default to Boston
                    
                    # Try to extract city from query
                    city_match = re.search(r"in\s+([a-zA-Z\s]+)(?:\s+with|\s+that|\?|$)", query, re.IGNORECASE)
                    if city_match:
                        city = city_match.group(1).strip()
                    
                    # Try to extract rating
                    min_score = 4.0  # Default
                    rating_match = re.search(r"(\d+\.?\d*)\s*(?:stars?|rating|\+)", query, re.IGNORECASE)
                    if rating_match:
                        min_score = float(rating_match.group(1))
                    
                    response = requests.get(
                        f"{API_URL}/search",
                        params={
                            "q": query,
                            "city": city,
                            "min_score": min_score,
                            "limit": result_limit
                        }
                    )
                
                # Process response
                if response.status_code == 200:
                    result = response.json()
                    
                    # Display generated SQL if applicable and show_sql is enabled
                    if show_sql and endpoint == "Natural Language (query)" and "sql_query" in result:
                        st.subheader("Generated SQL Query")
                        st.code(result['sql_query'], language="sql")
                    
                    # Format results based on endpoint
                    if endpoint == "Natural Language (query)":
                        if "results" in result and result["results"]:
                            results_data = result["results"]
                        else:
                            st.warning("No results found.")
                            st.stop()
                    else:
                        # For other endpoints, the response is the results list
                        results_data = result
                    
                    if not results_data:
                        st.warning("No results found for your query.")
                        st.stop()
                    
                    # Create DataFrame from results
                    df = pd.DataFrame(results_data)
                    
                    # Show results section
                    st.subheader(f"Found {len(df)} Restaurants")
                    
                    # Display metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Number of Results", len(df))
                    
                    if "stars" in df.columns:
                        with col2:
                            avg_rating = round(df["stars"].mean(), 2)
                            st.metric("Average Rating", f"{avg_rating} ★")
                    
                    if "food_score" in df.columns:
                        with col3:
                            avg_food = round(df["food_score"].mean(), 2)
                            st.metric("Avg Food Score", avg_food)
                    
                    if "service_score" in df.columns:
                        with col4:
                            avg_service = round(df["service_score"].mean(), 2)
                            st.metric("Avg Service Score", avg_service)
                    
                    # Results display options
                    view_option = st.radio("View as:", ["Table", "Cards"], horizontal=True)
                    
                    if view_option == "Table":
                        # Clean up dataframe for display
                        display_cols = ["name", "stars", "food_score", "service_score", "ambiance_score", "city", "address"]
                        display_df = df[display_cols] if all(col in df.columns for col in display_cols) else df
                        st.dataframe(display_df, use_container_width=True)
                    else:
                        # Card display
                        for i, row in df.iterrows():
                            col1, col2 = st.columns([3, 2])
                            with col1:
                                st.markdown(f"### {row['name']} - {row.get('stars', 'N/A')} ★")
                                st.markdown(f"**Address:** {row.get('address', 'N/A')}, {row.get('city', 'N/A')}")
                                if "price_range" in row:
                                    st.markdown(f"**Price Range:** {'$' * int(row['price_range'])}")
                            with col2:
                                # Create radar chart for scores
                                if all(x in row for x in ["food_score", "service_score", "ambiance_score"]):
                                    categories = ['Food', 'Service', 'Ambiance']
                                    values = [row['food_score'], row['service_score'], row['ambiance_score']]
                                    
                                    fig = go.Figure()
                                    fig.add_trace(go.Scatterpolar(
                                        r=values,
                                        theta=categories,
                                        fill='toself',
                                        name='Scores',
                                        line_color='#FF5A5F'
                                    ))
                                    
                                    fig.update_layout(
                                        polar=dict(
                                            radialaxis=dict(
                                                visible=True,
                                                range=[0, 5]
                                            )
                                        ),
                                        showlegend=False,
                                        height=200,
                                        margin=dict(l=10, r=10, t=10, b=10)
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                            
                            # Display reviews if available
                            if "reviews" in row and row["reviews"]:
                                with st.expander("Sample Reviews"):
                                    for review in row["reviews"][:3]:
                                        st.markdown(f"""<div class="review-card">
                                            <div>"{review['text'][:200]}..."</div>
                                            <div style="text-align: right; font-style: italic;">- {review.get('user_name', 'Anonymous')}</div>
                                        </div>""", unsafe_allow_html=True)
                            
                            st.divider()
                    
                    # Display restaurants on a map if coordinates available
                    if show_map and "latitude" in df.columns and "longitude" in df.columns:
                        st.subheader("Restaurant Locations")
                        st.map(df)
                    
                    # Score comparison chart if we have enough results
                    if len(df) > 1 and all(x in df.columns for x in ["food_score", "service_score", "ambiance_score"]):
                        st.subheader("Restaurant Score Comparison")
                        
                        # Prepare data for chart
                        chart_data = pd.melt(
                            df[["name", "food_score", "service_score", "ambiance_score"]].head(7),
                            id_vars=["name"],
                            value_vars=["food_score", "service_score", "ambiance_score"],
                            var_name="Score Type",
                            value_name="Score"
                        )
                        
                        # Replace column names for display
                        chart_data["Score Type"] = chart_data["Score Type"].replace({
                            "food_score": "Food", 
                            "service_score": "Service", 
                            "ambiance_score": "Ambiance"
                        })
                        
                        # Create grouped bar chart
                        fig = px.bar(
                            chart_data, 
                            x="name", 
                            y="Score", 
                            color="Score Type",
                            barmode="group",
                            height=400,
                            color_discrete_sequence=["#FF5A5F", "#00A699", "#FC642D"]
                        )
                        
                        fig.update_layout(
                            xaxis_title="Restaurant",
                            yaxis_title="Score (0-5)",
                            legend_title="Category",
                            xaxis={'categoryorder':'total descending'}
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                
                else:
                    st.error(f"Error: {response.status_code} - {response.text}")
            
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.info("Make sure your API server is running at " + API_URL)

with tab2:
    st.markdown("### Restaurant Analytics Dashboard")
    
    # Filters for the dashboard
    st.markdown("#### Filter Options")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        city_filter = st.selectbox(
            "City:",
            ["All Cities", "Boston", "New York", "Chicago", "San Francisco", "Los Angeles"]
        )
    with col2:
        rating_filter = st.slider(
            "Minimum Rating:",
            min_value=1.0,
            max_value=5.0,
            value=3.5,
            step=0.5
        )
    with col3:
        category_filter = st.multiselect(
            "Categories:",
            ["Cafe", "Restaurant", "Bakery", "Bar", "Fast Food"],
            default=["Cafe", "Restaurant"]
        )
    
    # Fetch dashboard data
    if st.button("Generate Dashboard", type="primary", key="dashboard_button"):
        with st.spinner("Fetching analytics data..."):
            try:
                # Get restaurant data from the API
                city_param = None if city_filter == "All Cities" else city_filter
                
                response = requests.get(
                    f"{API_URL}/restaurants",
                    params={
                        "city": city_param,
                        "min_score": rating_filter,
                        "limit": 50  # Get more data for analytics
                    }
                )
                
                if response.status_code == 200:
                    results = response.json()
                    if not results:
                        st.warning("No data available with the selected filters.")
                        st.stop()
                    
                    # Create DataFrame
                    df = pd.DataFrame(results)
                    
                    # Display data summary
                    st.markdown("### Restaurant Analytics Overview")
                    
                    # Summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Restaurants", len(df))
                    with col2:
                        avg_rating = round(df["stars"].mean(), 2) if "stars" in df.columns else "N/A"
                        st.metric("Average Rating", f"{avg_rating} ★")
                    with col3:
                        top_city = df["city"].value_counts().idxmax() if "city" in df.columns else "N/A"
                        st.metric("Top City", top_city)
                    with col4:
                        avg_food = round(df["food_score"].mean(), 2) if "food_score" in df.columns else "N/A"
                        st.metric("Avg Food Score", avg_food)
                    
                    # Create charts
                    st.markdown("### Score Distribution")
                    
                    # Score distribution charts
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Rating distribution histogram
                        if "stars" in df.columns:
                            fig = px.histogram(
                                df, 
                                x="stars", 
                                nbins=10,
                                color_discrete_sequence=["#FF5A5F"],
                                title="Star Rating Distribution"
                            )
                            
                            fig.update_layout(
                                xaxis_title="Star Rating",
                                yaxis_title="Number of Restaurants",
                                bargap=0.1
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        # Score category comparison
                        if all(x in df.columns for x in ["food_score", "service_score", "ambiance_score"]):
                            df_scores = pd.melt(
                                df[["food_score", "service_score", "ambiance_score"]],
                                value_vars=["food_score", "service_score", "ambiance_score"],
                                var_name="Category",
                                value_name="Score"
                            )
                            
                            # Convert column names for display
                            df_scores["Category"] = df_scores["Category"].replace({
                                "food_score": "Food", 
                                "service_score": "Service", 
                                "ambiance_score": "Ambiance"
                            })
                            
                            fig = px.box(
                                df_scores, 
                                x="Category", 
                                y="Score", 
                                color="Category",
                                color_discrete_sequence=["#FF5A5F", "#00A699", "#FC642D"],
                                title="Score Distribution by Category"
                            )
                            
                            fig.update_layout(
                                showlegend=False,
                                yaxis_range=[1, 5]
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                    
                    # Top restaurants section
                    st.markdown("### Top Rated Restaurants")
                    
                    # Sort by overall score
                    if "overall_score" in df.columns:
                        top_df = df.sort_values("overall_score", ascending=False).head(10)
                    else:
                        top_df = df.sort_values("stars", ascending=False).head(10)
                    
                    # Create bar chart for top restaurants
                    fig = px.bar(
                        top_df,
                        x="name",
                        y="stars" if "stars" in top_df.columns else "overall_score",
                        color="stars" if "stars" in top_df.columns else "overall_score",
                        title="Top 10 Restaurants by Rating",
                        color_continuous_scale=px.colors.sequential.Reds,
                        height=400
                    )
                    
                    fig.update_layout(
                        xaxis_title="Restaurant",
                        yaxis_title="Rating",
                        xaxis={'categoryorder':'total descending'}
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Map visualization if we have coordinates
                    if "latitude" in df.columns and "longitude" in df.columns:
                        st.markdown("### Restaurant Hotspots")
                        
                        fig = px.density_mapbox(
                            df, 
                            lat="latitude", 
                            lon="longitude", 
                            z="stars" if "stars" in df.columns else "overall_score", 
                            radius=10,
                            center=dict(lat=df["latitude"].mean(), lon=df["longitude"].mean()),
                            zoom=11,
                            mapbox_style="open-street-map",
                            title="Restaurant Rating Heatmap"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error(f"Error fetching data: {response.status_code} - {response.text}")
            
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.info("Make sure your API server is running at " + API_URL)

with tab3:
    st.markdown("### City Explorer")
    
    # City selection
    selected_city = st.selectbox(
        "Select a city to explore:",
        ["Boston", "New York", "Chicago", "San Francisco", "Los Angeles"]
    )
    
    # Information display
    if st.button("Explore City", type="primary", key="city_button"):
        with st.spinner(f"Exploring {selected_city}..."):
            try:
                # Fetch city-specific data
                response = requests.get(
                    f"{API_URL}/restaurants",
                    params={
                        "city": selected_city,
                        "limit": 50
                    }
                )
                
                if response.status_code == 200:
                    results = response.json()
                    if not results:
                        st.warning(f"No data available for {selected_city}.")
                        st.stop()
                    
                    # Create DataFrame
                    df = pd.DataFrame(results)
                    
                    # City overview
                    st.markdown(f"## Exploring {selected_city}")
                    
                    # Summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Restaurants", len(df))
                    with col2:
                        avg_rating = round(df["stars"].mean(), 2) if "stars" in df.columns else "N/A"
                        st.metric("Average Rating", f"{avg_rating} ★")
                    with col3:
                        avg_food = round(df["food_score"].mean(), 2) if "food_score" in df.columns else "N/A"
                        st.metric("Avg Food Score", avg_food)
                    with col4:
                        avg_service = round(df["service_score"].mean(), 2) if "service_score" in df.columns else "N/A"
                        st.metric("Avg Service Score", avg_service)
                    
                    # Top categories chart
                    st.markdown("### Popular Categories")
                    
                    if "categories" in df.columns:
                        # Extract all categories
                        all_categories = []
                        for cats in df["categories"]:
                            if isinstance(cats, str):
                                # Split categories and strip whitespace
                                split_cats = [c.strip() for c in cats.split(",")]
                                all_categories.extend(split_cats)
                        
                        # Count occurrences
                        if all_categories:
                            category_counts = pd.Series(all_categories).value_counts().head(10)
                            
                            # Create horizontal bar chart
                            fig = px.bar(
                                x=category_counts.values,
                                y=category_counts.index,
                                orientation="h",
                                color=category_counts.values,
                                color_continuous_scale=px.colors.sequential.Reds,
                                title=f"Top Categories in {selected_city}"
                            )
                            
                            fig.update_layout(
                                xaxis_title="Number of Restaurants",
                                yaxis_title="Category",
                                yaxis={'categoryorder':'total ascending'}
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                    
                    # Map of restaurants if we have coordinates
                    if "latitude" in df.columns and "longitude" in df.columns:
                        st.markdown(f"### {selected_city} Restaurant Map")
                        
                        # Create scatter mapbox
                        fig = px.scatter_mapbox(
                            df,
                            lat="latitude",
                            lon="longitude",
                            color="stars" if "stars" in df.columns else "overall_score",
                            size="stars" if "stars" in df.columns else "overall_score",
                            color_continuous_scale=px.colors.sequential.Reds,
                            size_max=15,
                            zoom=11,
                            mapbox_style="open-street-map",
                            hover_name="name",
                            hover_data=["address", "stars", "food_score", "service_score"],
                            title=f"{selected_city} Restaurant Map"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Top restaurants in the city
                    st.markdown(f"### Top Restaurants in {selected_city}")
                    
                    # Display top 5 as cards
                    top_5 = df.sort_values("stars" if "stars" in df.columns else "overall_score", ascending=False).head(5)
                    
                    for i, row in top_5.iterrows():
                        col1, col2 = st.columns([2, 3])
                        with col1:
                            st.markdown(f"### {i+1}. {row['name']}")
                            st.markdown(f"**Rating:** {row.get('stars', 'N/A')} ★")
                            st.markdown(f"**Address:** {row.get('address', 'N/A')}")
                            if "price_range" in row:
                                st.markdown(f"**Price:** {'$' * int(row['price_range'])}")
                        
                        with col2:
                            # Score comparison chart
                            if all(x in row for x in ["food_score", "service_score", "ambiance_score"]):
                                categories = ["Food", "Service", "Ambiance"]
                                scores = [row["food_score"], row["service_score"], row["ambiance_score"]]
                                
                                fig = px.bar(
                                    x=categories,
                                    y=scores,
                                    color=categories,
                                    color_discrete_sequence=["#FF5A5F", "#00A699", "#FC642D"],
                                    height=200
                                )
                                
                                fig.update_layout(
                                    showlegend=False,
                                    xaxis_title="",
                                    yaxis_title="Score",
                                    yaxis_range=[0, 5],
                                    margin=dict(l=10, r=10, t=10, b=10)
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                        
                        st.divider()
                
                else:
                    st.error(f"Error fetching city data: {response.status_code} - {response.text}")
            
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.info("Make sure your API server is running at " + API_URL)

with tab4:
    st.markdown("## About SentimentSift")
    
    st.markdown("""
    SentimentSift is an AI-powered restaurant analysis platform that analyzes customer reviews to provide meaningful insights about restaurants.
    
    ### How It Works
    
    1. **Data Collection**: We gather restaurant reviews from various sources
    2. **Sentiment Analysis**: Our AI models analyze each review to extract sentiment about food, service, and ambiance
    3. **Trend Analysis**: We identify trends and patterns in customer feedback
    4. **Visualization**: Interactive dashboards and visualizations make insights accessible
    
    ### Features
    
    - Natural language querying for restaurant recommendations
    - Sentiment analysis for detailed restaurant scoring
    - Visualization of restaurant data and trends
    - City exploration with interactive maps
    - Comparative analysis of restaurants
    
    ### Technology Stack
    
    - Frontend: Streamlit
    - Backend: FastAPI
    - Data Processing: Python (pandas, numpy)
    - Database: Snowflake
    - Sentiment Analysis: Custom NLP models
    """)

# Footer
st.markdown("""
<div class="footer">
    <p>© 2025 SentimentSift · Restaurant Analysis Platform</p>
</div>
""", unsafe_allow_html=True)