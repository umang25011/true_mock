"""
Base table model that handles data generation for a database table.
"""

import json
import os
from typing import Dict, List, Any, Optional
from .columns import Column

class TableModel:
    """
    Base class for table models.
    
    Each table model represents a database table and knows how to:
    1. Define its columns and their data types
    2. Generate realistic data for those columns
    3. Handle foreign key relationships through ReferenceColumns
    """
    
    def __init__(
        self,
        rows_per_table: int = None,
        batch_size: int = None,
        db_connector = None,
        config_file: str = None
    ):
        """
        Initialize a table model.
        
        Args:
            rows_per_table: Number of rows to generate (default from config)
            batch_size: Number of rows per batch (default from config)
            db_connector: Database connector for foreign key handling
            config_file: Path to config file (default is config.json)
        """
        # Core attributes
        self.columns: Dict[str, Column] = {}
        self.db_connector = db_connector
        
        # Load generation settings
        self._load_config(config_file)
        self.rows_per_table = rows_per_table or self.config['data_generation']['default_rows_per_table']
        self.batch_size = batch_size or self.config['data_generation']['default_batch_size']
        
        # Set up the model
        self._setup_columns()
        self._setup_foreign_keys()
    
    def _load_config(self, config_file: Optional[str] = None) -> None:
        """Load configuration from file."""
        if not config_file:
            config_file = os.path.join(os.path.dirname(__file__), 'config.json')
        
        with open(config_file, 'r') as f:
            self.config = json.load(f)
    
    def _setup_columns(self) -> None:
        """
        Define the table columns.
        
        Override this method in subclasses to define columns like:
            self.columns = {
                'id': IntegerColumn(nullable=False),
                'name': StringColumn(max_length=100),
                'email': EmailColumn()
            }
        """
        pass
    
    def _setup_foreign_keys(self) -> None:
        """Set up foreign key columns with database connector."""
        for column in self.columns.values():
            if hasattr(column, 'db_connector'):
                column.db_connector = self.db_connector
    
    def generate_row(self) -> Dict[str, Any]:
        """Generate a single row of data."""
        return {
            name: column.generate()
            for name, column in self.columns.items()
        }
    
    def generate_rows(self, count: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Generate multiple rows of data.
        
        Args:
            count: Number of rows to generate (default is rows_per_table)
        """
        count = count or self.rows_per_table
        return [self.generate_row() for _ in range(count)]
    
    def get_table_name(self) -> str:
        """Get the table name from the class name."""
        return self.__class__.__name__.replace('Table', '').lower()
    
    def get_column_names(self) -> List[str]:
        """Get list of column names."""
        return list(self.columns.keys())
    
    def __str__(self) -> str:
        """Human-readable representation of the table model."""
        lines = [
            f"Table Model: {self.__class__.__name__}",
            "-" * 40,
            "\nColumns:"
        ]
        
        # Show column details
        for name, column in self.columns.items():
            column_type = column.__class__.__name__
            nullable = "NULL" if column.nullable else "NOT NULL"
            unique = "UNIQUE" if getattr(column, 'unique', False) else ""
            lines.append(f"{name}: {column_type} {nullable} {unique}")
        
        # Show settings
        lines.extend([
            "\nGeneration Settings:",
            f"Rows per table: {self.rows_per_table}",
            f"Batch size: {self.batch_size}"
        ])
        
        return "\n".join(lines) 