"""
Base table model for data generation.
"""

from typing import Dict, List, Any


class TableModel:
    """Base class for table models."""
    
    def __init__(self):
        """Initialize table model."""
        self.columns: Dict[str, Any] = {}
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
    
    def generate_rows(self, count: int) -> List[Dict[str, Any]]:
        """Generate multiple rows of data."""
        return [self.generate_row() for _ in range(count)]
    
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