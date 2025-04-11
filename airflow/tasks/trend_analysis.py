# airflow/tasks/trend_analysis.py
import pandas as pd
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer
from typing import Dict, List, Any
import os
import datetime

def run_topic_modeling(review_paths: Dict[str, str], output_dir: str) -> Dict[str, Any]:
    """
    Run topic modeling on reviews for each business
    
    Args:
        review_paths: Dictionary mapping business_id to review file path
        output_dir: Directory to save output files
        
    Returns:
        Dictionary containing topic modeling results
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize BERTopic model
    vectorizer = CountVectorizer(stop_words="english")
    topic_model = BERTopic(vectorizer_model=vectorizer)
    
    business_topics = {}
    
    for business_id, review_path in review_paths.items():
        # Load reviews
        review_df = pd.read_csv(review_path)
        
        # Add a datetime column for time-based analysis
        review_df['datetime'] = pd.to_datetime(review_df['date'])
        
        # Sort by date
        review_df = review_df.sort_values('datetime')
        
        # Get review texts
        docs = review_df['text'].tolist()
        timestamps = review_df['datetime'].tolist()
        
        # Run topic modeling
        topics, _ = topic_model.fit_transform(docs)
        
        # Get topic info
        topic_info = topic_model.get_topic_info()
        
        # Get top topics
        top_topics = topic_info[topic_info['Count'] > 5].sort_values('Count', ascending=False)
        
        # Get topic keywords
        topic_keywords = {}
        for topic_id in top_topics['Topic'].tolist():
            if topic_id == -1:  # Skip outlier topic
                continue
            words = [word for word, _ in topic_model.get_topic(topic_id)]
            topic_keywords[topic_id] = words
        
        # Run topic over time analysis
        topics_over_time = topic_model.topics_over_time(docs, topics, timestamps)
        
        # Store results
        business_topics[business_id] = {
            'topic_model': topic_model,
            'topics': topics,
            'topic_info': topic_info,
            'topic_keywords': topic_keywords,
            'topics_over_time': topics_over_time
        }
        
        # Save topic info
        topic_info_path = os.path.join(output_dir, f'topic_info_{business_id}.csv')
        topic_info.to_csv(topic_info_path, index=False)
        
        # Save topics over time
        topics_over_time_path = os.path.join(output_dir, f'topics_over_time_{business_id}.csv')
        topics_over_time.to_csv(topics_over_time_path, index=False)
    
    # Create summary of trends
    trend_summary = []
    for business_id, results in business_topics.items():
        topic_info = results['topic_info']
        topic_keywords = results['topic_keywords']
        
        # Get top 3 topics
        top_3_topics = topic_info[topic_info['Topic'] != -1].sort_values('Count', ascending=False).head(3)
        
        for _, row in top_3_topics.iterrows():
            topic_id = row['Topic']
            count = row['Count']
            keywords = topic_keywords.get(topic_id, [])
            
            trend_summary.append({
                'business_id': business_id,
                'topic_id': topic_id,
                'count': count,
                'keywords': ', '.join(keywords[:5]),
                'name': f"Topic {topic_id}: {', '.join(keywords[:3])}"
            })
    
    # Create DataFrame with trend summary
    trend_summary_df = pd.DataFrame(trend_summary)
    
    # Save trend summary
    trend_summary_path = os.path.join(output_dir, 'trend_summary.csv')
    trend_summary_df.to_csv(trend_summary_path, index=False)
    
    return {
        'trend_summary_path': trend_summary_path,
        'business_topics': business_topics
    }

if __name__ == "__main__":
    # Example usage
    review_paths = {
        "business_id_1": "processed_data/reviews_business_id_1.csv",
        "business_id_2": "processed_data/reviews_business_id_2.csv",
    }
    output_dir = "trend_data"
    
    results = run_topic_modeling(review_paths, output_dir)
    print(f"Topic modeling results saved to {results['trend_summary_path']}")