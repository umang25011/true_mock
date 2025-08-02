"""
Core package for database schema extraction and model generation.
"""

from .schema_model import (
    SchemaModel, SchemaMetadata, Table, Column, ColumnType,
    ColumnFlags, ForeignKey, Reference, Index, ModelConfig,
    Relationship
)

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
    'Relationship'
] 