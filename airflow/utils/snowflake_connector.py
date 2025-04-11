# airflow/utils/snowflake_connector.py
import pandas as pd
import snowflake.connector
from typing import Dict, List, Any
import os

class SnowflakeConnector:
    def __init__(self, account: str, user: str, password: str, warehouse: str, database: str, schema: str):
        """
        Initialize Snowflake connector
        
        Args:
            account: Snowflake account
            user: Snowflake user
            password: Snowflake password
            warehouse: Snowflake warehouse
            database: Snowflake database
            schema: Snowflake schema
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
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
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a query
        
        Args:
            query: SQL query to execute
            
        Returns:
            DataFrame with query results
        """
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        cursor.execute(query)
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # Fetch results
        results = cursor.fetchall()
        
        # Create DataFrame
        df = pd.DataFrame(results, columns=column_names)
        
        cursor.close()
        
        return df
    
    def create_table(self, table_name: str, columns: Dict[str, str]):
        """
        Create a table
        
        Args:
            table_name: Name of table to create
            columns: Dictionary mapping column names to data types
        """
        if not self.conn:
            self.connect()
        
        column_defs = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs})"
        
        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()
    
    def insert_dataframe(self, table_name: str, df: pd.DataFrame):
        """
        Insert DataFrame into a table
        
        Args:
            table_name: Name of table to insert into
            df: DataFrame to insert
        """
        if not self.conn:
            self.connect()
        
        # Convert DataFrame to list of tuples
        values = [tuple(row) for row in df.values]
        
        # Generate placeholders for INSERT statement
        placeholders = ", ".join(["(%s)" % ", ".join(["%s"] * len(df.columns))])
        
        # Generate column names
        column_names = ", ".join(df.columns)
        
        # Generate INSERT statement
        query = f"INSERT INTO {table_name} ({column_names}) VALUES {placeholders}"
        
        cursor = self.conn.cursor()
        cursor.executemany(query, values)
        cursor.close()
    
    def update_dataframe(self, table_name: str, df: pd.DataFrame, key_column: str):
        """
        Update DataFrame in a table
        
        Args:
            table_name: Name of table to update
            df: DataFrame to update
            key_column: Column to use as key for update
        """
        if not self.conn:
            self.connect()
        
        # Delete existing rows
        keys = df[key_column].tolist()
        keys_str = ", ".join([f"'{key}'" for key in keys])
        delete_query = f"DELETE FROM {table_name} WHERE {key_column} IN ({keys_str})"
        
        cursor = self.conn.cursor()
        cursor.execute(delete_query)
        cursor.close()
        
        # Insert new rows
        self.insert_dataframe(table_name, df)

