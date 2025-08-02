"""
Schema package for database schema extraction and modeling.
"""

from .schema_model import (
    SchemaModel, SchemaMetadata, Table, Column, ColumnType,
    ColumnFlags, ForeignKey, Reference, Index, ModelConfig,
    Relationship
)
from .schema_extractor import SchemaExtractor

__all__ = [
    'SchemaModel',
    'SchemaMetadata',
    'Table',
    'Column',
    'ColumnType',
    'ColumnFlags',
    'ForeignKey',
    'Reference',
    'Index',
    'ModelConfig',
    'Relationship',
    'SchemaExtractor'
] 