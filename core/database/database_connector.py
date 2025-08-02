"""
Database connector module for SQL data generation.
Handles connection to PostgreSQL databases.
"""

import json
import logging
import os
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel


class DatabaseConfig(BaseModel):
    """Database configuration model."""
    type: str
    host: str
    port: int
    database: str
    username: str
    password: str
    db_schema: str = "public"


class Config(BaseModel):
    """Main configuration model."""
    database: DatabaseConfig


class DatabaseConnector:
    """Handles database connections."""
    
    def __init__(self, config_path: str = "../config.json"):
        """Initialize the database connector with configuration."""
        # Get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(script_dir, config_path)
        
        self.config = self._load_config(config_file)
        self.engine: Optional[Engine] = None
        
        # Setup basic logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_config(self, config_path: str) -> Config:
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                config_data = json.load(f)
            return Config(**config_data)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {config_path} not found")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON in configuration file {config_path}")
    
    def connect(self) -> bool:
        """Establish connection to the database."""
        try:
            db_config = self.config.database
            
            if db_config.type.lower() == "postgresql":
                connection_string = (
                    f"postgresql://{db_config.username}:{db_config.password}"
                    f"@{db_config.host}:{db_config.port}/{db_config.database}"
                )
            else:
                raise ValueError(f"Unsupported database type: {db_config.type}")
            
            self.engine = create_engine(connection_string, echo=False)
            
            self.logger.info(f"Successfully connected to {db_config.type} database: {db_config.database}")
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during connection: {e}")
            return False
    
    def get_engine(self) -> Optional[Engine]:
        """Get the SQLAlchemy engine."""
        return self.engine
    
    def get_schema(self) -> str:
        """Get the database schema."""
        return self.config.database.db_schema
    
    def is_connected(self) -> bool:
        """Check if the database is connected."""
        return self.engine is not None
    
    def close(self):
        """Close the database connection."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")


if __name__ == "__main__":
    # Example usage
    from sql_data_generator.core.db_operations import DatabaseOperations
    
    connector = DatabaseConnector()
    connector.connect()

    # Create operations instance with the connector
    db_ops = DatabaseOperations(connector)

    # Use operations
    tables = db_ops.get_tables()
    schema = db_ops.get_table_schema("some_table")

    # Close when done
    connector.close() 