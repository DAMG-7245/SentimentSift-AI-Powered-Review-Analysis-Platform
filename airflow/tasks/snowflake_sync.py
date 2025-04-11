# airflow/tasks/snowflake_sync.py
import pandas as pd
import os
from typing import Dict, List, Any
import sys
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.snowflake_connector import SnowflakeConnector

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load Snowflake configuration
    
    Args:
        config_path: Path to config file
        
    Returns:
        Dictionary with Snowflake configuration
    """
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config

def sync_business_data(connector: SnowflakeConnector, business_path: str, sentiment_path: str = None):
    """
    Sync business data to Snowflake
    
    Args:
        connector: SnowflakeConnector instance
        business_path: Path to business CSV file
        sentiment_path: Path to sentiment scores CSV file
    """
    # Load business data
    business_df = pd.read_csv(business_path)
    
    # Load sentiment data if provided
    if sentiment_path and os.path.exists(sentiment_path):
        sentiment_df = pd.read_csv(sentiment_path)
        
        # Merge business and sentiment data
        business_df = business_df.merge(sentiment_df, on='business_id', how='left')
    
    # Define table schema
    columns = {
        'business_id': 'VARCHAR(255) PRIMARY KEY',
        'name': 'VARCHAR(255)',
        'address': 'VARCHAR(255)',
        'city': 'VARCHAR(255)',
        'state': 'VARCHAR(50)',
        'postal_code': 'VARCHAR(20)',
        'latitude': 'FLOAT',
        'longitude': 'FLOAT',
        'stars': 'FLOAT',
        'review_count': 'INTEGER',
        'categories': 'VARCHAR(1000)',
        'service_score': 'FLOAT',
        'food_score': 'FLOAT',
        'ambiance_score': 'FLOAT',
        'overall_score': 'FLOAT'
    }
    
    # Create table if it doesn't exist
    connector.create_table('businesses', columns)
    
    # Update data
    connector.update_dataframe('businesses', business_df, 'business_id')
    
    print(f"Synced {len(business_df)} business records to Snowflake")

def sync_review_data(connector: SnowflakeConnector, review_paths: Dict[str, str]):
    """
    Sync review data to Snowflake
    
    Args:
        connector: SnowflakeConnector instance
        review_paths: Dictionary mapping business_id to review file path
    """
    # Define table schema
    columns = {
        'review_id': 'VARCHAR(255) PRIMARY KEY',
        'user_id': 'VARCHAR(255)',
        'business_id': 'VARCHAR(255)',
        'stars': 'FLOAT',
        'date': 'DATE',
        'text': 'TEXT',
        'useful': 'INTEGER',
        'funny': 'INTEGER',
        'cool': 'INTEGER'
    }
    
    # Create table if it doesn't exist
    connector.create_table('reviews', columns)
    
    # Load and sync review data for each business
    for business_id, review_path in review_paths.items():
        review_df = pd.read_csv(review_path)
        
        # Update data
        connector.update_dataframe('reviews', review_df, 'review_id')
        
        print(f"Synced {len(review_df)} review records for business {business_id} to Snowflake")

def sync_trend_data(connector: SnowflakeConnector, trend_summary_path: str):
    """
    Sync trend data to Snowflake
    
    Args:
        connector: SnowflakeConnector instance
        trend_summary_path: Path to trend summary CSV file
    """
    # Load trend data
    trend_df = pd.read_csv(trend_summary_path)
    
    # Define table schema
    columns = {
        'id': 'INTEGER IDENTITY(1,1) PRIMARY KEY',
        'business_id': 'VARCHAR(255)',
        'topic_id': 'INTEGER',
        'count': 'INTEGER',
        'keywords': 'VARCHAR(1000)',
        'name': 'VARCHAR(255)'
    }
    
    # Create table if it doesn't exist
    connector.create_table('trends', columns)
    
    # Remove id column if it exists (Snowflake will generate it)
    if 'id' in trend_df.columns:
        trend_df = trend_df.drop(columns=['id'])
    
    # Insert data (using insert instead of update since there's no natural key)
    connector.insert_dataframe('trends', trend_df)
    
    print(f"Synced {len(trend_df)} trend records to Snowflake")

def run_snowflake_sync(
    config_path: str, 
    business_path: str, 
    review_paths: Dict[str, str],
    sentiment_path: str = None,
    trend_summary_path: str = None
):
    """
    Run Snowflake sync process
    
    Args:
        config_path: Path to Snowflake config file
        business_path: Path to business CSV file
        review_paths: Dictionary mapping business_id to review file path
        sentiment_path: Path to sentiment scores CSV file
        trend_summary_path: Path to trend summary CSV file
    """
    # Load Snowflake configuration
    config = load_config(config_path)
    
    # Initialize Snowflake connector
    connector = SnowflakeConnector(
        account=config['account'],
        user=config['user'],
        password=config['password'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema']
    )
    
    try:
        # Connect to Snowflake
        connector.connect()
        
        # Sync business data
        sync_business_data(connector, business_path, sentiment_path)
        
        # Sync review data
        sync_review_data(connector, review_paths)
        
        # Sync trend data if provided
        if trend_summary_path and os.path.exists(trend_summary_path):
            sync_trend_data(connector, trend_summary_path)
        
    finally:
        # Disconnect from Snowflake
        connector.disconnect()
    
    print("Snowflake sync completed successfully")

if __name__ == "__main__":
    # Example usage
    config_path = "config/snowflake.json"
    business_path = "processed_data/selected_business.csv"
    review_paths = {
        "business_id_1": "processed_data/reviews_business_id_1.csv",
        "business_id_2": "processed_data/reviews_business_id_2.csv",
    }
    sentiment_path = "sentiment_data/sentiment_scores.csv"
    trend_summary_path = "trend_data/trend_summary.csv"
    
    run_snowflake_sync(config_path, business_path, review_paths, sentiment_path, trend_summary_path)