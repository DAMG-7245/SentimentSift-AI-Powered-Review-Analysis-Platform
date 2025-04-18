import os
import json
from typing import List, Dict, Any
from pinecone import Pinecone
from dotenv import load_dotenv
import google.generativeai as genai
from tqdm import tqdm

# Load environment variables
load_dotenv()

class ReviewSummarizationAgent:
    def __init__(self):
        """Initialize the RAG agent with Pinecone and Gemini API connections."""
        # Set up Pinecone
        pinecone_api_key = os.getenv('PINECONE_API_KEY')
        if not pinecone_api_key:
            raise ValueError("PINECONE_API_KEY environment variable not set")
        
        self.pc = Pinecone(api_key=pinecone_api_key)
        self.index_name = os.getenv('PINECONE_INDEX', 'reviews-index')
        self.index = self.pc.Index(self.index_name)
        
        # Set up Gemini
        gemini_api_key = os.getenv('GEMINI_API_KEY')
        if not gemini_api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")
        
        genai.configure(api_key=gemini_api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')
    
    def retrieve_reviews_by_business(self, business_id: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Retrieve all reviews for a specific business from Pinecone.
        
        Args:
            business_id: The ID of the business to retrieve reviews for
            limit: Maximum number of reviews to retrieve
            
        Returns:
            List of review dictionaries
        """
        # Query Pinecone for reviews with the specified business_id in metadata
        query_response = self.index.query(
            vector=[0] * 384,  # Dummy vector, we're using metadata filtering
            filter={"business_id": {"$eq": business_id}},
            top_k=limit,
            include_metadata=True
        )
        
        # Extract reviews from the response
        reviews = []
        for match in query_response.matches:
            reviews.append({
                'review_id': match.id,
                'review_text': match.metadata.get('review_text', ''),
                'business_id': match.metadata.get('business_id', '')
            })
        
        return reviews
    
    def generate_summary(self, reviews: List[Dict[str, Any]]) -> str:
        """
        Generate a summary of reviews using Gemini.
        
        Args:
            reviews: List of review dictionaries
            
        Returns:
            Generated summary
        """
        if not reviews:
            return "No reviews found for this business."
        
        # Prepare the prompt for Gemini
        review_texts = [review['review_text'] for review in reviews]
        
        prompt = f"""
        Analyze these business reviews and organize your findings into these sections:

        Food
        Positive Aspects: Common praise for flavor, freshness, preparation

        Negative Aspects: Recurring criticisms about food quality

        Ambiance
        Positive Aspects: Common praise for atmosphere, decor, noise level

        Negative Aspects: Recurring criticisms about atmosphere

        Service
        Positive Aspects: Common praise for staff behavior and efficiency

        Negative Aspects: Recurring criticisms about service quality

        Most Mentioned Food Items
        Only include items mentioned by 3+ reviewers

        Format as: "Item Name: Brief consensus"

        Focus on recurring themes and patterns, not isolated opinions. Be specific with food item names rather than general categories. Provide a concise yet comprehensive summary that captures the collective customer experience.
        {review_texts}
        """
        
        # Generate summary using Gemini
        response = self.model.generate_content(prompt)
        
        return response.text
    
    def summarize_business_reviews(self, business_id: str) -> str:
        """
        Retrieve and summarize reviews for a specific business.
        
        Args:
            business_id: The ID of the business to summarize reviews for
            
        Returns:
            Generated summary
        """
        print(f"Retrieving reviews for business: {business_id}")
        reviews = self.retrieve_reviews_by_business(business_id)
        print(f"Found {len(reviews)} reviews")
        
        if not reviews:
            return "No reviews found for this business."
        
        print("Generating summary...")
        summary = self.generate_summary(reviews)
        
        return summary

# Function to be called from Streamlit
def get_review_summary(business_id: str) -> str:
    """
    Function to be called from Streamlit to get a review summary for a business.
    
    Args:
        business_id: The ID of the business to summarize reviews for
        
    Returns:
        Generated summary
    """
    try:
        agent = ReviewSummarizationAgent()
        return agent.summarize_business_reviews(business_id)
    except Exception as e:
        return f"Error generating summary: {str(e)}"

def main(business_id=None):
    """
    Main function to run the review summarization process.
    Gets business_id from parameter or user input and returns the summary.
    
    Args:
        business_id: Optional business ID to summarize reviews for
        
    Returns:
        Generated summary
    """
    try:
        # Initialize the agent
        agent = ReviewSummarizationAgent()

        if business_id is None:
            business_id = input("Enter the business ID to summarize reviews for: ")
        
        # Generate and display summary
        summary = agent.summarize_business_reviews(business_id)
        
        print(summary)
        
        return summary
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return f"Error: {str(e)}"

if __name__ == "__main__":
    main()