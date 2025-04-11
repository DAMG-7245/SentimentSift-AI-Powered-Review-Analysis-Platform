# airflow/tasks/data_fetch.py
import requests
import json
import os
import time
from typing import List, Dict, Any
import pandas as pd

def fetch_yelp_data(api_key: str, location: str, categories: str, limit: int, output_dir: str) -> Dict[str, str]:
    """
    Fetch business and review data from Yelp API
    
    Args:
        api_key: Yelp API key
        location: Location to search for businesses
        categories: Categories to filter businesses
        limit: Maximum number of businesses to fetch
        output_dir: Directory to save output files
        
    Returns:
        Dictionary with paths to output files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Fetch businesses
    businesses = fetch_businesses(api_key, location, categories, limit)
    
    # Save businesses to JSON
    business_path = os.path.join(output_dir, 'businesses.json')
    with open(business_path, 'w', encoding='utf-8') as f:
        for business in businesses:
            f.write(json.dumps(business) + '\n')
    
    # Fetch reviews for each business
    reviews = []
    for business in businesses:
        business_reviews = fetch_reviews(api_key, business['id'])
        reviews.extend(business_reviews)
        
        # Add a delay to avoid rate limiting
        time.sleep(0.2)
    
    # Save reviews to JSON
    review_path = os.path.join(output_dir, 'reviews.json')
    with open(review_path, 'w', encoding='utf-8') as f:
        for review in reviews:
            f.write(json.dumps(review) + '\n')
    
    return {
        'business_path': business_path,
        'review_path': review_path
    }

def fetch_businesses(api_key: str, location: str, categories: str, limit: int) -> List[Dict[Any, Any]]:
    """
    Fetch businesses from Yelp API
    
    Args:
        api_key: Yelp API key
        location: Location to search for businesses
        categories: Categories to filter businesses
        limit: Maximum number of businesses to fetch
        
    Returns:
        List of business data dictionaries
    """
    url = 'https://api.yelp.com/v3/businesses/search'
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    businesses = []
    offset = 0
    
    while len(businesses) < limit:
        params = {
            'location': location,
            'categories': categories,
            'limit': min(50, limit - len(businesses)),
            'offset': offset,
            'sort_by': 'review_count',
            'radius': 40000  # 40km, maximum allowed
        }
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            result = response.json()
            batch = result.get('businesses', [])
            
            if not batch:
                break
                
            businesses.extend(batch)
            offset += len(batch)
            
            # Add a delay to avoid rate limiting
            time.sleep(0.2)
        else:
            print(f"Error fetching businesses: {response.status_code}")
            print(response.text)
            break
    
    # Format businesses to match JSON structure
    formatted_businesses = []
    for business in businesses:
        formatted_business = {
            'business_id': business.get('id'),
            'name': business.get('name'),
            'address': ' '.join(business.get('location', {}).get('display_address', [])),
            'city': business.get('location', {}).get('city'),
            'state': business.get('location', {}).get('state'),
            'postal_code': business.get('location', {}).get('zip_code'),
            'latitude': business.get('coordinates', {}).get('latitude'),
            'longitude': business.get('coordinates', {}).get('longitude'),
            'stars': business.get('rating'),
            'review_count': business.get('review_count'),
            'categories': ','.join([category.get('title') for category in business.get('categories', [])]),
            'attributes': json.dumps(business.get('attributes', {}))
        }
        formatted_businesses.append(formatted_business)
    
    return formatted_businesses

def fetch_reviews(api_key: str, business_id: str, limit: int = 100) -> List[Dict[Any, Any]]:
    """
    Fetch reviews for a business from Yelp API
    
    Args:
        api_key: Yelp API key
        business_id: Business ID to fetch reviews for
        limit: Maximum number of reviews to fetch
        
    Returns:
        List of review data dictionaries
    """
    url = f'https://api.yelp.com/v3/businesses/{business_id}/reviews'
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    reviews = []
    offset = 0
    
    while len(reviews) < limit:
        params = {
            'limit': min(50, limit - len(reviews)),
            'offset': offset
        }
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            result = response.json()
            batch = result.get('reviews', [])
            
            if not batch:
                break
                
            reviews.extend(batch)
            offset += len(batch)
            
            # Add a delay to avoid rate limiting
            time.sleep(0.2)
        else:
            print(f"Error fetching reviews: {response.status_code}")
            print(response.text)
            break
    
    # Format reviews to match JSON structure
    formatted_reviews = []
    for review in reviews:
        formatted_review = {
            'review_id': review.get('id'),
            'user_id': review.get('user', {}).get('id'),
            'business_id': business_id,
            'stars': review.get('rating'),
            'date': review.get('time_created', '').split(' ')[0],
            'text': review.get('text'),
            'useful': 0,
            'funny': 0,
            'cool': 0
        }
        formatted_reviews.append(formatted_review)
    
    return formatted_reviews

def fetch_yelp_data_updates(api_key: str, business_ids: List[str], output_dir: str) -> Dict[str, Any]:
    """
    Fetch updated review data for selected businesses
    
    Args:
        api_key: Yelp API key
        business_ids: List of business IDs to fetch updates for
        output_dir: Directory to save output files
        
    Returns:
        Dictionary with paths to output files and review paths
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Fetch business details for each business ID
    businesses = []
    for business_id in business_ids:
        business = fetch_business_details(api_key, business_id)
        if business:
            businesses.append(business)
        
        # Add a delay to avoid rate limiting
        time.sleep(0.2)
    
    # Save businesses to JSON
    business_path = os.path.join(output_dir, 'businesses_update.json')
    with open(business_path, 'w', encoding='utf-8') as f:
        for business in businesses:
            f.write(json.dumps(business) + '\n')
    
    # Save businesses to CSV
    business_df = pd.DataFrame(businesses)
    business_csv_path = os.path.join(output_dir, 'businesses_update.csv')
    business_df.to_csv(business_csv_path, index=False)
    
    # Fetch reviews for each business
    review_paths = {}
    for business in businesses:
        business_id = business['business_id']
        business_reviews = fetch_reviews(api_key, business_id)
        
        # Save reviews to JSON
        review_json_path = os.path.join(output_dir, f'reviews_{business_id}.json')
        with open(review_json_path, 'w', encoding='utf-8') as f:
            for review in business_reviews:
                f.write(json.dumps(review) + '\n')
        
        # Save reviews to CSV
        review_df = pd.DataFrame(business_reviews)
        review_csv_path = os.path.join(output_dir, f'reviews_{business_id}.csv')
        review_df.to_csv(review_csv_path, index=False)
        
        review_paths[business_id] = review_csv_path
    
    return {
        'business_path': business_csv_path,
        'review_paths': review_paths
    }

def fetch_business_details(api_key: str, business_id: str) -> Dict[Any, Any]:
    """
    Fetch details for a business from Yelp API
    
    Args:
        api_key: Yelp API key
        business_id: Business ID to fetch details for
        
    Returns:
        Dictionary with business details
    """
    url = f'https://api.yelp.com/v3/businesses/{business_id}'
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        business = response.json()
        
        # Format business to match JSON structure
        formatted_business = {
            'business_id': business.get('id'),
            'name': business.get('name'),
            'address': ' '.join(business.get('location', {}).get('display_address', [])),
            'city': business.get('location', {}).get('city'),
            'state': business.get('location', {}).get('state'),
            'postal_code': business.get('location', {}).get('zip_code'),
            'latitude': business.get('coordinates', {}).get('latitude'),
            'longitude': business.get('coordinates', {}).get('longitude'),
            'stars': business.get('rating'),
            'review_count': business.get('review_count'),
            'categories': ','.join([category.get('title') for category in business.get('categories', [])]),
            'attributes': json.dumps(business.get('attributes', {}))
        }
        
        return formatted_business
    else:
        print(f"Error fetching business details: {response.status_code}")
        print(response.text)
        return None

if __name__ == "__main__":
    # Example usage
    api_key = "YOUR_YELP_API_KEY"
    location = "Boston, MA"
    categories = "restaurants"
    limit = 10
    output_dir = "raw_data"
    
    result = fetch_yelp_data(api_key, location, categories, limit, output_dir)
    print(f"Data saved to {result['business_path']} and {result['review_path']}")