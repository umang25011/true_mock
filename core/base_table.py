"""
Base table model for data generation.
"""

import json
import os
from typing import Dict, List, Any
import logging

class TableModel:
    """Base class for table models."""
    
    def __init__(self, rows_per_table: int = None, batch_size: int = None):
        """Initialize table model with configuration."""
        self.columns: Dict[str, Any] = {}
        
        # Load config
        config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Set generation parameters
        self.rows_per_table = rows_per_table or self.config['data_generation']['default_rows_per_table']
        self.batch_size = batch_size or self.config['data_generation']['default_batch_size']
        
        # Set up logging
        logging.basicConfig(
            level=self.config['logging']['level'],
            filename=self.config['logging']['file'],
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize columns
        self._setup_columns()
    
    def _setup_columns(self):
        """
        Override this method to define table columns.
        Example:
            self.columns = {
                'id': IntegerColumn(nullable=False),
                'name': StringColumn(max_length=100)
            }
        """
        pass
    
    def generate_row(self) -> Dict[str, Any]:
        """Generate a single row of data."""
        row = {}
        for name, column in self.columns.items():
            row[name] = column.generate()
        return row
    
    def generate_rows(self, count: int = None) -> List[Dict[str, Any]]:
        """Generate multiple rows of data."""
        count = count or self.rows_per_table
        return [self.generate_row() for _ in range(count)]
    
    def get_table_name(self) -> str:
        """Get the table name from the class name."""
        return self.__class__.__name__.replace('Table', '').lower()
    
    def get_column_names(self) -> List[str]:
        """Get list of column names."""
        return list(self.columns.keys())
    
    def __str__(self) -> str:
        """String representation showing table structure."""
        lines = [f"Table Model: {self.__class__.__name__}"]
        lines.append("-" * 40)
        for name, column in self.columns.items():
            column_type = column.__class__.__name__
            nullable = "NULL" if column.nullable else "NOT NULL"
            unique = "UNIQUE" if column.unique else ""
            lines.append(f"{name}: {column_type} {nullable} {unique}")
        return "\n".join(lines) 