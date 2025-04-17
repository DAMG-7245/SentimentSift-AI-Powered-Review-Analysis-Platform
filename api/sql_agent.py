import snowflake.connector
import pandas as pd
from typing import Dict, List, Any, Tuple, Optional
import json
import re
import os
from dotenv import load_dotenv

class SQLAgent:
    def __init__(self, snowflake_config_path: str):
        """
        Initialize SQL Agent with Snowflake configuration
        
        Args:
            snowflake_config_path: Path to Snowflake config file
        """
        # Load Snowflake configuration
        load_dotenv()
        
        # 嘗試從環境變量獲取配置
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database = os.getenv("SNOWFLAKE_DATABASE")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA")
        
        self.conn = None
    
    def connect(self):
        """
        Connect to Snowflake
        """
        try:
            self.conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            return True
        except Exception as e:
            print(f"Snowflake connection error: {str(e)}")
            return False
    
    def disconnect(self):
        """
        Disconnect from Snowflake
        """
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a SQL query with parameters
        
        Args:
            query: SQL query to execute
            params: Dictionary of parameters for the query
            
        Returns:
            Tuple of (results as list of dictionaries, column names)
        """
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        
        try:
            # Execute query with parameters if provided
            if params:
                cursor.execute(query, params)
            else:
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
            
            return rows, column_names
        
        except Exception as e:
            print(f"Query execution error: {str(e)}")
            raise e
        
        finally:
            cursor.close()
    
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
        Enhanced version: Convert natural language question to SQL query with improved detection
        
        Args:
            question: Natural language question from user
        
        Returns:
            SQL query corresponding to the natural language question
        """
        question = question.lower()
        
        # Extract city information using regex
        city_pattern = r"in\s+([a-zA-Z\s]+)(?:\s+with|\s+and|\s+that|\s*$|\s*\?)"
        city_match = re.search(city_pattern, question)
        city = None
        if city_match:
            # Extract city name and clean it
            city = city_match.group(1).strip()
            # Remove trailing words that might not be part of the city
            city = re.sub(r'\s+with.*$|\s+that.*$|\s+and.*$|\s+having.*$', '', city)
        
        # Extract minimum rating information using regex
        rating_pattern = r"(\d+(?:\.\d+)?)\s*(?:star|stars|rating|or above|or higher|\+)"
        rating_match = re.search(rating_pattern, question)
        min_rating = None
        if rating_match:
            # Convert matched rating to float
            min_rating = float(rating_match.group(1))
        
        # Check for specific cities - extend this list as needed
        cities = ["boston", "new york", "chicago", "los angeles", "san francisco"]
        detected_city = city
        if not detected_city:
            # If no city was extracted with regex, check for city names directly
            for c in cities:
                if c in question:
                    detected_city = c
                    break
        
        # Determine if we're asking for a count or aggregation
        is_count_query = any(word in question for word in ["how many", "count", "number of"])
        is_avg_query = any(word in question for word in ["average", "avg", "mean"])
        is_top_query = any(word in question for word in ["top", "best", "highest"])
        
        # Build the base query
        if is_count_query:
            query = "SELECT COUNT(*) as restaurant_count FROM restaurants r WHERE 1=1"
        elif is_avg_query:
            query = """
            SELECT 
                AVG(r.stars) as avg_rating, 
                AVG(r.food_score) as avg_food_score, 
                AVG(r.service_score) as avg_service_score, 
                AVG(r.ambiance_score) as avg_ambiance_score
            FROM restaurants r
            WHERE 1=1
            """
        else:
            query = """
            SELECT 
                r.name, r.address, r.city, r.stars, r.overall_score, 
                r.food_score, r.service_score, r.ambiance_score,
                r.latitude, r.longitude, r.price_range, r.categories
            FROM restaurants r
            WHERE 1=1
            """
        
        # Add city filter if detected
        if detected_city:
            # Capitalize city name correctly (e.g., "New York" instead of "new york")
            formatted_city = ' '.join(word.capitalize() for word in detected_city.split())
            query += f" AND r.city = '{formatted_city}'"
        
        # Add rating filter if detected
        if min_rating:
            query += f" AND r.stars >= {min_rating}"
        
        # Check for restaurant type in the question
        if "restaurant" in question or "restaurants" in question or "places to eat" in question:
            query += " AND r.categories LIKE '%Restaurant%'"
        
        # Check for cafe/coffee mentions
        if "cafe" in question or "cafes" in question or "coffee" in question:
            query += " AND r.categories LIKE '%Cafe%'"
        
        # Check for specific cuisine keywords
        cuisines = ["italian", "chinese", "mexican", "japanese", "thai", "indian", "french"]
        for cuisine in cuisines:
            if cuisine in question:
                query += f" AND r.categories LIKE '%{cuisine.capitalize()}%'"
        
        # Check for food mentions
        if "food" in question or "delicious" in question:
            if is_top_query:
                query += " AND r.food_score >= 4.0"
            query += " ORDER BY r.food_score DESC"
        # Check for service mentions
        elif "service" in question or "staff" in question or "waiter" in question:
            if is_top_query:
                query += " AND r.service_score >= 4.0"
            query += " ORDER BY r.service_score DESC"
        # Check for ambiance mentions
        elif "ambiance" in question or "atmosphere" in question or "romantic" in question:
            if is_top_query:
                query += " AND r.ambiance_score >= 4.0"
            query += " ORDER BY r.ambiance_score DESC"
        # Default ordering
        else:
            if is_top_query:
                query += " AND r.overall_score >= 4.0"
            query += " ORDER BY r.overall_score DESC"
        
        # Check for specific ambiance/environment preferences
        if "romantic" in question:
            query += " AND r.ambiance_score >= 4.0"
        
        if any(word in question for word in ["family", "kid", "kids", "children"]):
            query += " AND r.is_family_friendly = TRUE"
        
        # Limit the number of results
        limit = 10  # Default limit
        limit_pattern = r"(?:show|get|find|list)\s+(?:top\s+)?(\d+)"
        limit_match = re.search(limit_pattern, question)
        if limit_match:
            limit = int(limit_match.group(1))
        
        # If it's not an aggregation query, add limit
        if not is_avg_query:
            query += f" LIMIT {limit}"
        
        return query
    
    def get_restaurant_stats(self, city: Optional[str] = None) -> Dict[str, Any]:
        """
        Get restaurant statistics for dashboard
        
        Args:
            city: Optional city to filter results
            
        Returns:
            Dictionary with statistics
        """
        stats = {}
        
        try:
            # Build the base query
            query = """
            SELECT 
                COUNT(*) as total_restaurants,
                AVG(stars) as avg_rating,
                AVG(food_score) as avg_food_score,
                AVG(service_score) as avg_service_score,
                AVG(ambiance_score) as avg_ambiance_score
            FROM 
                restaurants
            """
            
            # Add city filter if specified
            if city:
                query += f" WHERE city = '{city}'"
            
            # Execute query
            results, _ = self.execute_query(query)
            
            if results and len(results) > 0:
                stats["summary"] = results[0]
            
            # Get rating distribution
            rating_query = """
            SELECT 
                FLOOR(stars * 2) / 2 as rating_bin,
                COUNT(*) as count
            FROM 
                restaurants
            """
            
            if city:
                rating_query += f" WHERE city = '{city}'"
            
            rating_query += """
            GROUP BY 
                rating_bin
            ORDER BY 
                rating_bin
            """
            
            rating_results, _ = self.execute_query(rating_query)
            stats["rating_distribution"] = rating_results
            
            # Get top restaurants
            top_query = """
            SELECT 
                name, 
                stars, 
                food_score, 
                service_score, 
                ambiance_score
            FROM 
                restaurants
            """
            
            if city:
                top_query += f" WHERE city = '{city}'"
            
            top_query += """
            ORDER BY 
                overall_score DESC
            LIMIT 10
            """
            
            top_results, _ = self.execute_query(top_query)
            stats["top_restaurants"] = top_results
            
            return stats
            
        except Exception as e:
            print(f"Error getting stats: {str(e)}")
            return {"error": str(e)}
    
    def get_category_distribution(self, city: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get distribution of restaurant categories
        
        Args:
            city: Optional city to filter results
            
        Returns:
            List of category counts
        """
        try:
            # For Snowflake, we'd use SPLIT_TO_TABLE or similar
            # For simplicity, this example does basic category extraction
            query = """
            SELECT 
                TRIM(c.value) as category, 
                COUNT(*) as count
            FROM 
                restaurants r,
                TABLE(SPLIT_TO_TABLE(r.categories, ',')) c
            """
            
            if city:
                query += f" WHERE r.city = '{city}'"
            
            query += """
            GROUP BY 
                TRIM(c.value)
            ORDER BY 
                count DESC
            LIMIT 15
            """
            
            results, _ = self.execute_query(query)
            return results
            
        except Exception as e:
            print(f"Error getting category distribution: {str(e)}")
            return []