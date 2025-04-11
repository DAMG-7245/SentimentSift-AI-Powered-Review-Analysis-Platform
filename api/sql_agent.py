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