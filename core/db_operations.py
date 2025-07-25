"""
Database operations module.
Handles all database queries and schema inspection operations.
"""

import logging
from typing import Dict, List, Optional, Any
from sqlalchemy import MetaData, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


class DatabaseOperations:
    """Handles all database operations and schema inspection."""
    
    def __init__(self, db_connector):
        """Initialize database operations with database connector."""
        if not db_connector.is_connected():
            raise RuntimeError("Database not connected. Call connect() first.")
            
        self.db = db_connector
        self.engine = db_connector.get_engine()
        self.schema = db_connector.get_schema()
        self.metadata = MetaData()
        self.inspector = inspect(self.engine)
        self.logger = logging.getLogger(__name__)
    
    def test_connection(self) -> bool:
        """Test if the database connection is working."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("Database connection test successful")
            return True
        except SQLAlchemyError as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        try:
            tables = self.inspector.get_table_names(schema=self.schema)
            self.logger.info(f"Found {len(tables)} tables: {tables}")
            return tables
        except Exception as e:
            self.logger.error(f"Error getting tables: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get detailed schema information for a specific table."""
        try:
            columns = self.inspector.get_columns(table_name, schema=self.schema)
            primary_keys = self.inspector.get_pk_constraint(table_name, schema=self.schema)
            foreign_keys = self.inspector.get_foreign_keys(table_name, schema=self.schema)
            indexes = self.inspector.get_indexes(table_name, schema=self.schema)
            
            schema_info = {
                "table_name": table_name,
                "columns": columns,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys,
                "indexes": indexes
            }
            
            self.logger.info(f"Retrieved schema for table: {table_name}")
            return schema_info
            
        except Exception as e:
            self.logger.error(f"Error getting schema for table {table_name}: {e}")
            return {}
    
    def get_all_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Get schema information for all tables."""
        tables = self.get_tables()
        schemas = {}
        
        for table in tables:
            schemas[table] = self.get_table_schema(table)
        
        return schemas
    
    def get_foreign_key_relationships(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all foreign key relationships between tables."""
        relationships = {}
        tables = self.get_tables()
        
        for table in tables:
            foreign_keys = self.inspector.get_foreign_keys(table, schema=self.schema)
            if foreign_keys:
                relationships[table] = foreign_keys
        
        self.logger.info(f"Found foreign key relationships: {relationships}")
        return relationships
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.schema}.{table_name}"))
                count = result.scalar()
                self.logger.info(f"Table {table_name} has {count} rows")
                return count
        except Exception as e:
            self.logger.error(f"Error getting row count for table {table_name}: {e}")
            return 0
    
    def get_all_table_counts(self) -> Dict[str, int]:
        """Get row counts for all tables."""
        tables = self.get_tables()
        counts = {}
        
        for table in tables:
            counts[table] = self.get_table_row_count(table)
        
        return counts
    
    def get_table_sample_data(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get sample data from a table."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {self.schema}.{table_name} LIMIT {limit}"))
                columns = result.keys()
                data = [dict(zip(columns, row)) for row in result.fetchall()]
                self.logger.info(f"Retrieved {len(data)} sample rows from table {table_name}")
                return data
        except Exception as e:
            self.logger.error(f"Error getting sample data from table {table_name}: {e}")
            return [] 