# api/sql_agent.py
import snowflake.connector
import pandas as pd
from typing import Dict, List, Any, Tuple
import json
import re

class SQLAgent:
    def __init__(self, snowflake_config_path: str):
        """
        Initialize SQL Agent with Snowflake configuration
        
        Args:
            snowflake_config_path: Path to Snowflake config file
        """
        # Load Snowflake configuration
        with open(snowflake_config_path, 'r') as f:
            config = json.load(f)
        
        self.account = config['account']
        self.user = config['user']
        self.password = config['password']
        self.warehouse = config['warehouse']
        self.database = config['database']
        self.schema = config['schema']
        
        # Initialize connection
        self.conn = None
    
    def connect(self):
        """
        Connect to Snowflake
        """
        self.conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
    
    def disconnect(self):
        """
        Disconnect from Snowflake
        """
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def execute_query(self, query: str) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a SQL query
        
        Args:
            query: SQL query to execute
            
        Returns:
            Tuple of (results as list of dictionaries, column names)
        """
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        cursor.execute(query)
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # Fetch results
        results = cursor.fetchall()
        
        # Convert to list of dictionaries
        rows = []
        for row in results:
            row_dict = {}
            for i, col in enumerate(column_names):
                row_dict[col] = row[i]
            rows.append(row_dict)
        
        cursor.close()
        
        return rows, column_names
    
    def natural_language_query(self, question: str) -> Dict[str, Any]:
        """
        Process a natural language query and convert to SQL
        
        Args:
            question: Natural language question
            
        Returns:
            Dictionary with query results and metadata
        """
        # Convert natural language to SQL
        sql_query = self.nl_to_sql(question)
        
        # Execute SQL query
        results, columns = self.execute_query(sql_query)
        
        return {
            'question': question,
            'sql_query': sql_query,
            'results': results,
            'columns': columns
        }
    
    def nl_to_sql(self, question: str) -> str:
        """
        Convert natural language question to SQL query
        
        Args:
            question: Natural language question
            
        Returns:
            SQL query
        """
        # This is a simple rule-based approach
        # In a production environment, you might want to use a more sophisticated NLP approach
        question = question.lower()
        
        # Check for boston restaurants
        if "boston" in question and ("restaurant" in question or "restaurants" in question):
            if "best" in question or "top" in question or "highest" in question:
                return """
                SELECT b.name, b.address, b.stars, b.overall_score, b.food_score, b.service_score, b.ambiance_score
                FROM businesses b
                WHERE b.city = 'Boston' AND b.categories LIKE '%Restaurant%'
                ORDER BY b.overall_score DESC
                LIMIT 10
                """
        
        # Check for cafes
        if "cafe" in question or "cafes" in question or "coffee" in question:
            if "best" in question or "top" in question or "highest" in question:
                return """
                SELECT b.name, b.address, b.stars, b.overall_score, b.food_score, b.service_score, b.ambiance_score
                FROM businesses b
                WHERE b.categories LIKE '%Cafe%' OR b.categories LIKE '%Coffee%'
                ORDER BY b.overall_score DESC
                LIMIT 10
                """
        
        # Check for specific cuisine
        cuisines = ["italian", "chinese", "mexican", "japanese", "thai", "indian", "french"]
        for cuisine in cuisines:
            if cuisine in question:
                return f"""
                SELECT b.name, b.address, b.stars, b.overall_score, b.food_score, b.service_score, b.ambiance_score
                FROM businesses b
                WHERE b.categories LIKE '%{cuisine.capitalize()}%'
                ORDER BY b.overall_score DESC
                LIMIT 10
                """
        
        # Check for specific aspects
        aspects = {
            "food": "food_score",
            "service": "service_score",
            "ambiance": "ambiance_score"
        }
        for aspect, column in aspects.items():
            if aspect in question:
                return f"""
                SELECT b.name, b.address, b.stars, b.overall_score, b.{column}
                FROM businesses b
                ORDER BY b.{column} DESC
                LIMIT 10
                """
        
        # Default query for restaurants
        return """
        SELECT b.name, b.address, b.city, b.stars, b.overall_score
        FROM businesses b
        ORDER BY b.overall_score DESC
        LIMIT 10
        """

# api/app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import os
import json
from sql_agent import SQLAgent

app = FastAPI(title="Yelp Sentiment Analysis API")

# Load configuration
with open("../config.py", "r") as f:
    config = json.load(f)

# Initialize SQL Agent
sql_agent = SQLAgent(config["snowflake_config_path"])

class NLQuery(BaseModel):
    question: str

class QueryResult(BaseModel):
    question: str
    sql_query: str
    results: List[Dict[str, Any]]
    columns: List[str]

@app.post("/query", response_model=QueryResult)
async def natural_language_query(query: NLQuery):
    """
    Process a natural language query about restaurants
    """
    try:
        result = sql_agent.natural_language_query(query.question)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/restaurants", response_model=List[Dict[str, Any]])
async def get_top_restaurants(limit: int = 10, city: Optional[str] = None):
    """
    Get top restaurants based on overall score
    """
    try:
        sql_query = """
        SELECT b.business_id, b.name, b.address, b.city, b.stars, 
               b.overall_score, b.food_score, b.service_score, b.ambiance_score
        FROM businesses b
        """
        
        if city:
            sql_query += f" WHERE b.city = '{city}'"
        
        sql_query += f"""
        ORDER BY b.overall_score DESC
        LIMIT {limit}
        """
        
        results, _ = sql_agent.execute_query(sql_query)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/restaurants/{business_id}/reviews", response_model=List[Dict[str, Any]])
async def get_restaurant_reviews(business_id: str, limit: int = 20):
    """
    Get reviews for a specific restaurant
    """
    try:
        sql_query = f"""
        SELECT r.review_id, r.stars, r.date, r.text, r.useful, r.funny, r.cool
        FROM reviews r
        WHERE r.business_id = '{business_id}'
        ORDER BY r.date DESC
        LIMIT {limit}
        """
        
        results, _ = sql_agent.execute_query(sql_query)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/restaurants/{business_id}/trends", response_model=List[Dict[str, Any]])
async def get_restaurant_trends(business_id: str, limit: int = 5):
    """
    Get topic trends for a specific restaurant
    """
    try:
        sql_query = f"""
        SELECT t.topic_id, t.count, t.keywords, t.name
        FROM trends t
        WHERE t.business_id = '{business_id}'
        ORDER BY t.count DESC
        LIMIT {limit}
        """
        
        results, _ = sql_agent.execute_query(sql_query)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search", response_model=List[Dict[str, Any]])
async def search_restaurants(
    q: str, 
    city: Optional[str] = None, 
    min_score: Optional[float] = None,
    limit: int = 10
):
    """
    Search restaurants by name or category
    """
    try:
        sql_query = """
        SELECT b.business_id, b.name, b.address, b.city, b.stars, 
               b.overall_score, b.food_score, b.service_score, b.ambiance_score
        FROM businesses b
        WHERE (b.name ILIKE '%"""+ q +"""%' OR b.categories ILIKE '%"""+ q +"""%')
        """
        
        if city:
            sql_query += f" AND b.city = '{city}'"
        
        if min_score:
            sql_query += f" AND b.overall_score >= {min_score}"
        
        sql_query += f"""
        ORDER BY b.overall_score DESC
        LIMIT {limit}
        """
        
        results, _ = sql_agent.execute_query(sql_query)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)