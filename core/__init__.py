"""
Core package containing base classes and utilities.
"""

# Import base classes first
from .base_table import TableModel

# Import column types
from .columns import (
    Column,
    IntegerColumn,
    StringColumn,
    DateTimeColumn,
    EmailColumn,
    NameColumn,
    PhoneColumn,
    ReferenceColumn
)

# Define what's available at package level
__all__ = [
    'TableModel',
    'Column',
    'IntegerColumn',
    'StringColumn',
    'DateTimeColumn',
    'EmailColumn',
    'NameColumn',
    'PhoneColumn',
    'ReferenceColumn'
] 