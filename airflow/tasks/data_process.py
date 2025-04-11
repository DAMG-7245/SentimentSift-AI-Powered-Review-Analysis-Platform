# airflow/tasks/data_process.py
import json
import pandas as pd
import os
from typing import List, Dict, Any

def load_json_file(file_path: str) -> List[Dict[Any, Any]]:
    """
    Load JSON file into a list of dictionaries
    """
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))
    return data

def process_business_data(data: List[Dict[Any, Any]], category_filter: str = 'Restaurants') -> pd.DataFrame:
    """
    Process business data to create a business table
    
    Args:
        data: List of business data dictionaries
        category_filter: Category to filter businesses
        
    Returns:
        DataFrame containing filtered business data
    """
    # Filter for restaurants
    restaurants = []
    for business in data:
        if business.get('categories') and category_filter in business.get('categories'):
            restaurants.append(business)
    
    # Create DataFrame
    business_df = pd.DataFrame(restaurants)
    
    # Select relevant columns
    if not business_df.empty:
        business_df = business_df[[
            'business_id', 'name', 'address', 'city', 'state', 
            'postal_code', 'latitude', 'longitude', 'stars', 
            'review_count', 'attributes', 'categories'
        ]]
    
    return business_df

def process_review_data(data: List[Dict[Any, Any]], business_ids: List[str]) -> Dict[str, pd.DataFrame]:
    """
    Process review data to create review tables for each business
    
    Args:
        data: List of review data dictionaries
        business_ids: List of business IDs to filter reviews
        
    Returns:
        Dictionary of DataFrames containing review data for each business
    """
    # Create a dictionary to store reviews for each business
    business_reviews = {business_id: [] for business_id in business_ids}
    
    # Filter reviews for selected businesses
    for review in data:
        business_id = review.get('business_id')
        if business_id in business_ids:
            business_reviews[business_id].append(review)
    
    # Create DataFrames for each business
    review_dfs = {}
    for business_id, reviews in business_reviews.items():
        if reviews:
            review_df = pd.DataFrame(reviews)
            review_df = review_df[[
                'review_id', 'user_id', 'business_id', 'stars', 
                'date', 'text', 'useful', 'funny', 'cool'
            ]]
            review_dfs[business_id] = review_df
    
    return review_dfs

def select_top_reviewed_businesses(business_df: pd.DataFrame, min_reviews: int = 500, count: int = 5) -> List[str]:
    """
    Select top reviewed businesses
    
    Args:
        business_df: DataFrame containing business data
        min_reviews: Minimum number of reviews required
        count: Number of businesses to select
        
    Returns:
        List of selected business IDs
    """
    # Filter businesses with minimum reviews
    filtered_df = business_df[business_df['review_count'] >= min_reviews]
    
    # Sort by review count in descending order and take top 'count'
    top_businesses = filtered_df.sort_values('review_count', ascending=False).head(count)
    
    return top_businesses['business_id'].tolist()

def run_data_processing(business_path: str, review_path: str, output_dir: str) -> Dict[str, Any]:
    """
    Run the data processing pipeline
    
    Args:
        business_path: Path to business JSON file
        review_path: Path to review JSON file
        output_dir: Directory to save output files
        
    Returns:
        Dictionary containing metadata and paths to processed data
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Load business data
    business_data = load_json_file(business_path)
    business_df = process_business_data(business_data)
    
    # Save business dataframe
    business_output_path = os.path.join(output_dir, 'business.csv')
    business_df.to_csv(business_output_path, index=False)
    
    # Select top 5 restaurants with at least 500 reviews
    selected_business_ids = select_top_reviewed_businesses(business_df)
    
    # Filter business dataframe to only include selected businesses
    selected_business_df = business_df[business_df['business_id'].isin(selected_business_ids)]
    selected_business_output_path = os.path.join(output_dir, 'selected_business.csv')
    selected_business_df.to_csv(selected_business_output_path, index=False)
    
    # Load review data
    review_data = load_json_file(review_path)
    
    # Process reviews for selected businesses
    review_dfs = process_review_data(review_data, selected_business_ids)
    
    # Save review dataframes
    review_paths = {}
    for business_id, review_df in review_dfs.items():
        review_output_path = os.path.join(output_dir, f'reviews_{business_id}.csv')
        review_df.to_csv(review_output_path, index=False)
        review_paths[business_id] = review_output_path
    
    # Return metadata
    return {
        'business_path': business_output_path,
        'selected_business_path': selected_business_output_path,
        'selected_business_ids': selected_business_ids,
        'review_paths': review_paths
    }

if __name__ == "__main__":
    # Example usage
    business_path = "data/yelp_dataset/business.json"
    review_path = "data/yelp_dataset/review.json"
    output_dir = "data/processed_data"
    
    metadata = run_data_processing(business_path, review_path, output_dir)
    print(f"Processed data saved to {output_dir}")
    print(f"Selected {len(metadata['selected_business_ids'])} businesses")