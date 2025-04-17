# api/chatbot_agent.py
import httpx
import json
from typing import List, Dict, Any, Optional
import re

class ChatbotAgent:
    """
    Agent responsible for handling restaurant-related queries through the chatbot interface.
    Integrates with the SQL API to fetch structured data.
    """
    
    def __init__(self, api_base_url: str = "http://localhost:8000"):
        """
        Initialize the chatbot agent with API connection details.
        
        Args:
            api_base_url: Base URL for the restaurant API endpoints
        """
        self.api_base_url = api_base_url
        
    async def find_restaurants_by_location_and_rating(self, location: str, min_rating: float = 4.0) -> List[Dict[str, Any]]:
        """
        Query restaurants by location and minimum rating.
        
        Args:
            location: City name (e.g., "New York")
            min_rating: Minimum rating threshold (default: 4.0)
            
        Returns:
            List of restaurant data matching the criteria
        """
        params = {
            "city": location,
            "min_score": min_rating,
            "limit": 10
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.api_base_url}/search", params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                # Handle error cases
                return []
    
    def parse_restaurant_query(self, query: str) -> Dict[str, Any]:
        """
        Parse natural language query to extract location and rating parameters.
        
        Args:
            query: Natural language query string
            
        Returns:
            Dictionary with extracted parameters
        """
        query = query.lower()
        params = {}
        
        # Extract location (city)
        location_patterns = [
            r"in\s+([a-zA-Z\s]+)",
            r"at\s+([a-zA-Z\s]+)",
            r"(?:restaurants|places)(?:\s+in|\s+at)\s+([a-zA-Z\s]+)"
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, query)
            if match:
                # Clean up extracted city name
                city = match.group(1).strip()
                # Remove trailing words that might not be part of the city
                city = re.sub(r'\s+with.*$|\s+that.*$|\s+and.*$', '', city)
                params["location"] = city
                break
        
        # Extract minimum rating
        rating_patterns = [
            r"(\d+\.?\d*)\s+stars?",
            r"rating\s+(?:of|above|over)\s+(\d+\.?\d*)",
            r"rated\s+(?:above|over)\s+(\d+\.?\d*)"
        ]
        
        for pattern in rating_patterns:
            match = re.search(pattern, query)
            if match:
                params["min_rating"] = float(match.group(1))
                break
        
        return params
    
    async def process_restaurant_query(self, query: str) -> str:
        """
        Process natural language restaurant query and return formatted results.
        
        Args:
            query: Natural language query string
            
        Returns:
            Formatted response with restaurant recommendations
        """
        # Parse query to extract parameters
        params = self.parse_restaurant_query(query)
        
        # Set defaults if parameters are missing
        location = params.get("location")
        min_rating = params.get("min_rating", 4.0)
        
        if not location:
            return "I need a location to find restaurants. Could you specify a city?"
        
        # Fetch restaurants matching criteria
        restaurants = await self.find_restaurants_by_location_and_rating(location, min_rating)
        
        if not restaurants:
            return f"I couldn't find any restaurants in {location} with a rating of {min_rating} or higher."
        
        # Format response
        response = f"Here are top-rated restaurants in {location} with {min_rating}+ stars:\n\n"
        
        for i, restaurant in enumerate(restaurants, 1):
            response += f"{i}. {restaurant['name']} - {restaurant['stars']} stars\n"
            response += f"   Address: {restaurant['address']}\n"
            if 'food_score' in restaurant:
                response += f"   Scores: Food ({restaurant['food_score']}), Service ({restaurant['service_score']}), Ambiance ({restaurant['ambiance_score']})\n"
            response += "\n"
        
        return response

def create_restaurant_tools(chatbot_agent):
    """
    Create tools for restaurant queries.
    
    Args:
        chatbot_agent: Instance of ChatbotAgent
        
    Returns:
        List of tool objects
    """
    return [{
        "name": "RestaurantFinder",
        "description": "Find restaurants by location and rating. Input should be a question about restaurants in a specific location."
    }]