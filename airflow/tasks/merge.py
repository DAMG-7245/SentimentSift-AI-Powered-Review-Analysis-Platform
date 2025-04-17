import json
import csv
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def ensure_directory_exists(file_path):
    """Ensure the directory exists; if not, create it."""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        logger.info(f"Creating directory: {directory}")
        os.makedirs(directory)

def integrate_data(business_file, sentiment_file, topic_file, output_file):
    """
    Integrate business information, sentiment analysis, and topic modeling data for cafes.
    
    Args:
        business_file (str): Path to the JSON file with basic business information.
        sentiment_file (str): Path to the JSON file with sentiment analysis results.
        topic_file (str): Path to the CSV file with topic modeling results.
        output_file (str): Path to output the merged data as a JSON file.
    """
    try:
        # Ensure output directory exists
        ensure_directory_exists(output_file)
        
        # Load business data
        logger.info(f"Loading business data from: {business_file}")
        with open(business_file, 'r', encoding='utf-8') as f:
            business_data = json.load(f)
        
        # Load sentiment analysis data
        logger.info(f"Loading sentiment data from: {sentiment_file}")
        with open(sentiment_file, 'r', encoding='utf-8') as f:
            sentiment_data = json.load(f)
        
        # Load topic modeling data
        logger.info(f"Loading topic data from: {topic_file}")
        topic_data = []
        with open(topic_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                topic_data.append(row)
        
        logger.info(f"Loaded {len(business_data)} businesses, {len(sentiment_data)} sentiment records, {len(topic_data)} topic records")
        
        # Create sentiment mapping (business_id -> scores)
        sentiment_mapping = {}
        for record in sentiment_data:
            sentiment_mapping[record['business_id']] = record['scores']
        
        # Create topic mapping (business_id -> [topics])
        topic_mapping = {}
        for record in topic_data:
            business_id = record['business_id']
            
            if business_id not in topic_mapping:
                topic_mapping[business_id] = []
            
            # Attempt to convert count to integer
            try:
                count = int(record['count'])
            except ValueError:
                count = record['count']
                
            topic_mapping[business_id].append({
                'topic_id': record['topic_id'],
                'count': count,
                'keywords': record['keywords'],
                'name': record['name']
            })
        
        # Integrate data
        for business in business_data:
            business_id = business['business_id']
            
            # Add sentiment scores
            if business_id in sentiment_mapping:
                business['sentiment_scores'] = sentiment_mapping[business_id]
            
            # Add topic data
            if business_id in topic_mapping:
                business['topics'] = topic_mapping[business_id]
        
        # Save integrated data
        logger.info(f"Saving integrated data to: {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(business_data, f, ensure_ascii=False, indent=2)
        
        return business_data
        
    except Exception as e:
        logger.error(f"Error during data integration: {str(e)}", exc_info=True)
        raise

# File paths - adjust according to your project structure
if __name__ == "__main__":
    business_file = "data/processed/business.json"
    sentiment_file = "data/sentiment/cafe_sentiment_with_tiers.json"  
    topic_file = "data/trend_data/trend_summary.csv"  
    output_file = "data/merge/integrated_business.json"
    
    try:
        integrated_data = integrate_data(business_file, sentiment_file, topic_file, output_file)
        
        print(f"Integrated data for {len(integrated_data)} businesses")
        
        # Count businesses with sentiment and topic data
        with_sentiment = sum(1 for b in integrated_data if 'sentiment_scores' in b)
        with_topics = sum(1 for b in integrated_data if 'topics' in b)
        
        print(f"Businesses with sentiment scores: {with_sentiment}")
        print(f"Businesses with topic data: {with_topics}")
        
        # Print an example
        if integrated_data:
            print("\nExample of the first business:")
            first_business = integrated_data[0]
            print(f"Name: {first_business['name']}")
            print(f"Address: {first_business['full_address']}")
            
            if 'sentiment_scores' in first_business:
                scores = first_business['sentiment_scores']
                print(f"Service: {scores['service']} ({scores['service_tier']})")
                print(f"Food: {scores['food']} ({scores['food_tier']})")
                print(f"Ambiance: {scores['ambiance']} ({scores['ambiance_tier']})")
            
            if 'topics' in first_business:
                print("Main topics:")
                for topic in first_business['topics']:
                    print(f"  - {topic['name']} (Mentions: {topic['count']})")
        
        print(f"\nIntegration complete! Data saved to: {output_file}")
    
    except Exception as e:
        print(f"An error occurred during execution: {str(e)}")
