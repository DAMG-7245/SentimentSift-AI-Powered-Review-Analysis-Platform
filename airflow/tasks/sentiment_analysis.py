# airflow/tasks/sentiment_analysis.py
import pandas as pd
import numpy as np
import torch
from transformers import RobertaTokenizer, RobertaForSequenceClassification
from typing import List, Dict, Any
import os
import re

class RestaurantSentimentAnalyzer:
    def __init__(self, model_name: str = "roberta-base"):
        """
        Initialize the sentiment analyzer with a pre-trained model
        
        Args:
            model_name: Name of the pre-trained model to use
        """
        self.tokenizer = RobertaTokenizer.from_pretrained(model_name)
        self.model = RobertaForSequenceClassification.from_pretrained(model_name)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)
        self.model.eval()
        
        # Define aspect keywords
        self.aspect_keywords = {
            'service': ['service', 'staff', 'waiter', 'waitress', 'server', 'host', 'hostess', 'manager', 'customer service'],
            'food': ['food', 'dish', 'meal', 'taste', 'flavor', 'delicious', 'menu', 'portion', 'ingredient', 'appetizer', 'entree', 'dessert'],
            'ambiance': ['ambiance', 'atmosphere', 'decor', 'environment', 'music', 'noise', 'seating', 'comfort', 'cleanliness', 'clean', 'interior', 'design']
        }
    
    def extract_aspect_sentences(self, text: str, aspect: str) -> List[str]:
        """
        Extract sentences related to a specific aspect
        
        Args:
            text: Review text
            aspect: Aspect to extract sentences for
            
        Returns:
            List of sentences related to the aspect
        """
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        aspect_sentences = []
        keywords = self.aspect_keywords[aspect]
        
        for sentence in sentences:
            sentence_lower = sentence.lower()
            if any(keyword in sentence_lower for keyword in keywords):
                aspect_sentences.append(sentence)
        
        return aspect_sentences
    
    def analyze_sentiment(self, text: str) -> float:
        """
        Analyze the sentiment of a text
        
        Args:
            text: Text to analyze
            
        Returns:
            Sentiment score (0-1)
        """
        inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512, padding=True)
        inputs = {key: val.to(self.device) for key, val in inputs.items()}
        
        with torch.no_grad():
            outputs = self.model(**inputs)
            logits = outputs.logits
            
        # Convert logits to probabilities
        probs = torch.nn.functional.softmax(logits, dim=-1)
        positive_score = probs[0][1].item()
        
        return positive_score
    
    def analyze_aspect(self, review: str, aspect: str) -> float:
        """
        Analyze sentiment for a specific aspect
        
        Args:
            review: Review text
            aspect: Aspect to analyze
            
        Returns:
            Sentiment score (0-10) for the aspect
        """
        aspect_sentences = self.extract_aspect_sentences(review, aspect)
        
        if not aspect_sentences:
            return 5.0  # Neutral score if no sentences found
        
        # Analyze sentiment for each sentence
        scores = [self.analyze_sentiment(sentence) for sentence in aspect_sentences]
        
        # Average score and scale to 0-10
        avg_score = np.mean(scores) if scores else 0.5
        scaled_score = avg_score * 10
        
        return scaled_score
    
    def analyze_reviews(self, reviews: List[str]) -> Dict[str, float]:
        """
        Analyze reviews for all aspects
        
        Args:
            reviews: List of review texts
            
        Returns:
            Dictionary of average scores for each aspect
        """
        aspects = ['service', 'food', 'ambiance']
        aspect_scores = {aspect: [] for aspect in aspects}
        
        for review in reviews:
            for aspect in aspects:
                score = self.analyze_aspect(review, aspect)
                aspect_scores[aspect].append(score)
        
        # Calculate average scores
        avg_scores = {aspect: np.mean(scores) if scores else 5.0 
                     for aspect, scores in aspect_scores.items()}
        
        # Add overall score (average of all aspects)
        avg_scores['overall'] = np.mean(list(avg_scores.values()))
        
        return avg_scores

def run_sentiment_analysis(review_paths: Dict[str, str], output_dir: str) -> Dict[str, Any]:
    """
    Run sentiment analysis on reviews for each business
    
    Args:
        review_paths: Dictionary mapping business_id to review file path
        output_dir: Directory to save output files
        
    Returns:
        Dictionary containing sentiment scores for each business
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize sentiment analyzer
    analyzer = RestaurantSentimentAnalyzer()
    
    # Analyze sentiment for each business
    business_scores = {}
    
    for business_id, review_path in review_paths.items():
        # Load reviews
        review_df = pd.read_csv(review_path)
        
        # Analyze sentiment
        reviews = review_df['text'].tolist()
        scores = analyzer.analyze_reviews(reviews)
        
        # Store scores
        business_scores[business_id] = scores
    
    # Create a DataFrame with sentiment scores
    sentiment_data = []
    for business_id, scores in business_scores.items():
        row = {'business_id': business_id}
        for aspect, score in scores.items():
            row[f'{aspect}_score'] = score
        sentiment_data.append(row)
    
    sentiment_df = pd.DataFrame(sentiment_data)
    
    # Save sentiment scores
    sentiment_output_path = os.path.join(output_dir, 'sentiment_scores.csv')
    sentiment_df.to_csv(sentiment_output_path, index=False)
    
    return {
        'sentiment_path': sentiment_output_path,
        'business_scores': business_scores
    }

if __name__ == "__main__":
    # Automatically locate review files
    import os
    from pathlib import Path

    # Get the project root directory
    current_dir = Path(__file__).parent
    project_dir = current_dir.parent.parent

    # Define data directories
    processed_dir = project_dir / "data/processed_data"
    output_dir = project_dir / "data/sentiment_data"

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Automatically get all review files
    review_paths = {}
    for file_path in processed_dir.glob("reviews_*.csv"):
        business_id = file_path.stem.replace("reviews_", "")
        review_paths[business_id] = str(file_path)

    if not review_paths:
        print(f"Error: No review files found in {processed_dir}")
        print("Please run data_process.py first to generate review files.")
        exit(1)

    print(f"Found {len(review_paths)} review files for businesses:")
    for business_id, path in review_paths.items():
        print(f"  - {business_id}: {path}")

    # Run sentiment analysis
    results = run_sentiment_analysis(review_paths, str(output_dir))
    print(f"Sentiment analysis results saved to {results['sentiment_path']}")

    # Print scoring results
    print("\nBusiness sentiment scores:")
    for business_id, scores in results['business_scores'].items():
        print(f"Business ID: {business_id}")
        for aspect, score in scores.items():
            print(f"  {aspect}: {score:.2f}")
        print()
