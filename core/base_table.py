"""
Base table model for data generation.
"""

import json
import os
from typing import Dict, List, Any, Optional
import logging
from .relations import ModelRegistry, Relation

class TableModel:
    """Base class for table models."""
    
    def __init__(self):
        """Initialize table model."""
        self.columns: Dict[str, Any] = {}
        self.relations: List[Relation] = []
        self.model_registry: Optional[ModelRegistry] = None
        self._setup_columns()
        self._setup_relations()
    
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

    def _setup_relations(self):
        """
        Override this method to define table relationships.
        Example:
            self.relations.append(
                ManyToOneRelation(
                    from_table="employee",
                    to_table="department",
                    from_column="dept_id",
                    to_column="id"
                )
            )
        """
        pass
    
    def set_model_registry(self, registry: ModelRegistry):
        """Set the model registry for relationship handling."""
        self.model_registry = registry
    
    def generate_row(self) -> Dict[str, Any]:
        """Generate a single row of data."""
        row = {}
        
        # First generate values for non-relation columns
        for name, column in self.columns.items():
            if not any(name == rel.from_column for rel in self.relations):
                row[name] = column.generate()
        
        # Then handle relationships to ensure referenced data exists
        if self.model_registry:
            for relation in self.relations:
                row[relation.from_column] = relation.generate_related_data(self.model_registry)
        
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
        
        # Show columns
        lines.append("\nColumns:")
        for name, column in self.columns.items():
            column_type = column.__class__.__name__
            nullable = "NULL" if column.nullable else "NOT NULL"
            unique = "UNIQUE" if getattr(column, 'unique', False) else ""
            lines.append(f"{name}: {column_type} {nullable} {unique}")
        
        # Show relationships
        if self.relations:
            lines.append("\nRelationships:")
            for rel in self.relations:
                rel_type = rel.__class__.__name__.replace('Relation', '')
                lines.append(
                    f"{rel.from_column} -> {rel.to_table}.{rel.to_column} "
                    f"({rel_type})"
                )
        
        return "\n".join(lines) 