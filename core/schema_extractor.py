"""
Schema extractor that generates a comprehensive JSON representation of database schema.
"""

import json
from typing import Dict, List, Any, Optional
from sqlalchemy import inspect, text
from datetime import datetime

from schema_model import (
    SchemaModel, SchemaMetadata, Table, Column, ColumnType,
    ColumnFlags, ForeignKey, Reference, Index, ModelConfig,
    Relationship
)

class SchemaExtractor:
    """Extracts detailed database schema information into JSON format."""

    def __init__(self, db_ops):
        """Initialize with database operations instance."""
        self.db_ops = db_ops
        self.inspector = inspect(self.db_ops.db.get_engine())
        self.schema = self.db_ops.db.get_schema()

    def _get_sqlalchemy_type(self, column: Dict[str, Any]) -> str:
        """Map SQL type to SQLAlchemy type name."""
        type_str = str(column['type']).lower()
        
        # Handle special cases first
        if hasattr(column['type'], 'enums'):
            return 'Enum'
        
        type_map = {
            'bigint': 'BigInteger',
            'integer': 'Integer',
            'smallint': 'SmallInteger',
            'varchar': 'String',
            'char': 'String',
            'text': 'Text',
            'boolean': 'Boolean',
            'date': 'Date',
            'timestamp': 'DateTime',
            'numeric': 'Numeric',
            'float': 'Float',
            'real': 'Float'
        }
        
        for sql_type, alchemy_type in type_map.items():
            if sql_type in type_str:
                return alchemy_type
        
        return 'String'  # Safe default

    def _get_python_type(self, sqlalchemy_type: str) -> str:
        """Map SQLAlchemy type to Python type hint."""
        type_map = {
            'BigInteger': 'int',
            'Integer': 'int',
            'SmallInteger': 'int',
            'String': 'str',
            'Text': 'str',
            'Boolean': 'bool',
            'Date': 'date',
            'DateTime': 'datetime',
            'Numeric': 'Decimal',
            'Float': 'float',
            'Enum': 'str'
        }
        return type_map.get(sqlalchemy_type, 'Any')

    def _get_column_details(self, table_name: str, column: Dict[str, Any], table_constraints: Dict[str, Any]) -> Column:
        """Extract detailed information about a column."""
        col_type = str(column['type'])
        type_params = {}

        # Extract type-specific parameters
        if 'enum' in col_type.lower() or (
            hasattr(column['type'], 'enums') and 
            getattr(column['type'], 'enums', None)
        ):
            enum_values = getattr(column['type'], 'enums', None)
            if not enum_values:
                start = col_type.find('(')
                end = col_type.rfind(')')
                if start != -1 and end != -1:
                    enum_values = [
                        v.strip().strip("'").strip('"') 
                        for v in col_type[start + 1:end].split(',')
                    ]
            if enum_values:
                type_params['enum_values'] = enum_values

        elif hasattr(column['type'], 'length'):
            type_params['length'] = column['type'].length

        elif hasattr(column['type'], 'precision'):
            type_params['precision'] = column['type'].precision
            if hasattr(column['type'], 'scale'):
                type_params['scale'] = column['type'].scale

        # Create column type
        column_type = ColumnType(
            python=column['type'].__class__.__name__,
            sql=col_type,
            sqlalchemy_type=self._get_sqlalchemy_type(column),
            python_type=self._get_python_type(self._get_sqlalchemy_type(column)),
            params=type_params if type_params else None
        )

        # Create column flags
        flags = ColumnFlags(
            nullable=column.get('nullable', True),
            primary_key=column.get('primary_key', False),
            auto_increment=column.get('autoincrement', False) or 'nextval' in str(column.get('default', '')).lower(),
            unique=column.get('unique', False)
        )

        # Check if column is part of primary key
        if table_constraints['primary_key']:
            if column['name'] in table_constraints['primary_key']['columns']:
                flags.primary_key = True
                flags.nullable = False
                if len(table_constraints['primary_key']['columns']) > 1:
                    flags.composite_primary_key = True
                    flags.composite_position = table_constraints['primary_key']['columns'].index(column['name']) + 1

        # Check unique constraints
        for const in table_constraints['unique']:
            if column['name'] in const['columns']:
                flags.unique = True
                if len(const['columns']) > 1:
                    if flags.composite_unique is None:
                        flags.composite_unique = []
                    flags.composite_unique.append({
                        'name': const['name'],
                        'columns': const['columns'],
                        'position': const['columns'].index(column['name']) + 1
                    })

        return Column(
            name=column['name'],
            type=column_type,
            flags=flags,
            default=str(column['default']) if column.get('default') is not None else None
        )

    def _get_index_details(self, table_name: str) -> List[Index]:
        """Get details about table indexes."""
        indexes = []
        for idx in self.inspector.get_indexes(table_name, schema=self.schema):
            indexes.append(Index(
                name=idx['name'],
                columns=idx['column_names'],
                unique=idx['unique'],
                type=idx.get('type', 'btree')
            ))
        return indexes

    def _get_constraint_details(self, table_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """Get details about table constraints."""
        constraints = {
            'unique': [],
            'check': [],
            'primary_key': None
        }

        # Get unique constraints
        for const in self.inspector.get_unique_constraints(table_name, schema=self.schema):
            constraints['unique'].append({
                'name': const['name'],
                'columns': const['column_names']
            })

        # Get primary key constraint
        pk = self.inspector.get_pk_constraint(table_name, schema=self.schema)
        if pk['constrained_columns']:
            constraints['primary_key'] = {
                'name': pk.get('name'),
                'columns': pk['constrained_columns']
            }

        # Get check constraints if supported by dialect
        try:
            for const in self.inspector.get_check_constraints(table_name, schema=self.schema):
                constraints['check'].append({
                    'name': const['name'],
                    'sqltext': const.get('sqltext', '')
                })
        except NotImplementedError:
            pass  # Check constraints not supported by this dialect

        return constraints

    def _process_foreign_keys(self, schema_data: SchemaModel) -> None:
        """Process foreign key relationships and update column references."""
        for table_name, table in schema_data.tables.items():
            fks = self.inspector.get_foreign_keys(table_name, schema=self.schema)
            
            for fk in fks:
                for local_col, remote_col in zip(
                    fk['constrained_columns'],
                    fk['referred_columns']
                ):
                    # Add reference to the local column
                    for col in table.columns:
                        if col.name == local_col:
                            # Add foreign key info
                            col.foreign_key = ForeignKey(
                                table=fk['referred_table'],
                                column=remote_col,
                                name=fk['name'],
                                onupdate=fk.get('onupdate') if fk.get('onupdate') != 'NO ACTION' else None,
                                ondelete=fk.get('ondelete') if fk.get('ondelete') != 'NO ACTION' else None
                            )
                            # Update flags
                            col.flags.foreign_key = True
                    
                    # Update referenced column
                    remote_table = schema_data.tables[fk['referred_table']]
                    for col in remote_table.columns:
                        if col.name == remote_col:
                            if col.referenced_by is None:
                                col.referenced_by = []
                            col.referenced_by.append(Reference(
                                table=table_name,
                                column=local_col
                            ))
                            col.flags.is_referenced = True

    def _analyze_relationships(self, table_info: Table, schema_data: SchemaModel) -> List[Relationship]:
        """Analyze table relationships based on foreign keys."""
        relationships = []
        
        # Track processed relationships to avoid duplicates
        processed = set()
        
        # Check each column's foreign keys and references
        for col in table_info.columns:
            # belongs_to relationships (foreign keys)
            if col.flags.foreign_key and col.foreign_key:
                rel_key = f"belongs_to:{col.foreign_key.table}"
                if rel_key not in processed:
                    processed.add(rel_key)
                    relationships.append(Relationship(
                        type='belongs_to',
                        target=col.foreign_key.table,
                        foreign_key=col.name,
                        local_field=col.foreign_key.table.lower(),
                        back_populates=table_info.name.lower()
                    ))
            
            # has_many/has_one relationships
            if col.flags.is_referenced and col.referenced_by:
                for ref in col.referenced_by:
                    rel_key = f"has_many:{ref.table}"
                    rev_key = f"belongs_to:{table_info.name}"
                    
                    if rel_key not in processed and rev_key not in processed:
                        processed.add(rel_key)
                        
                        # Check if unique reference
                        ref_table = schema_data.tables[ref.table]
                        is_unique = any(
                            col.flags.unique
                            for col in ref_table.columns
                            if col.name == ref.column
                        )
                        
                        relationships.append(Relationship(
                            type='has_one' if is_unique else 'has_many',
                            target=ref.table,
                            foreign_key=ref.column,
                            local_field=ref.table.lower() + ('_one' if is_unique else '_many'),
                            back_populates=table_info.name.lower()
                        ))
        
        return relationships

    def _get_model_config(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        """Generate model configuration information."""
        config = {
            'table_args': {
                'schema': self.schema
            }
        }

        # Get primary keys
        if table_info['constraints']['primary_key']:
            config['primary_keys'] = table_info['constraints']['primary_key']['columns']

        # Get unique constraints (simplified)
        unique_columns = []
        for const in table_info['constraints']['unique']:
            if len(const['columns']) == 1:  # Only single-column unique constraints
                unique_columns.extend(const['columns'])
        for idx in table_info.get('indexes', []):
            if idx['unique'] and len(idx['columns']) == 1:
                unique_columns.extend(idx['columns'])
        if unique_columns:
            config['unique_constraints'] = list(set(unique_columns))

        return config

    def _get_table_details(self, table_name: str) -> Table:
        """Get comprehensive details about a table."""
        # Get constraints first
        constraints = self._get_constraint_details(table_name)
        
        # Get columns with constraint info
        columns = []
        for col in self.inspector.get_columns(table_name, schema=self.schema):
            column = self._get_column_details(table_name, col, constraints)
            columns.append(column)

        # Get indexes
        indexes = self._get_index_details(table_name)

        # Create model config
        model_config = ModelConfig(
            table_name=table_name,
            schema=self.schema,
            primary_keys=constraints['primary_key']['columns'] if constraints['primary_key'] else []
        )

        return Table(
            name=table_name,
            schema=self.schema,
            columns=columns,
            model_config=model_config,
            relationships=[],  # Will be populated later
            indexes=indexes if indexes else None
        )

    def _get_schema_metadata(self) -> Dict[str, Any]:
        """Get metadata about the schema itself."""
        return {
            'name': self.schema,
            'dialect': self.inspector.engine.dialect.name,
            'version': str(self.inspector.engine.dialect.server_version_info),
            'generated_at': datetime.now().isoformat()
        }

    def _get_sequence_info(self, column: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract sequence information from column default."""
        default = column.get('default')
        if not default:
            return None

        default_str = str(default).lower()
        if 'nextval' not in default_str:
            return None

        # Extract sequence name from nextval expression
        # Example: "nextval('schema.sequence_name'::regclass)"
        try:
            start = default_str.find("'") + 1
            end = default_str.rfind("'")
            if start > 0 and end > start:
                sequence_name = default_str[start:end]
                # Remove schema prefix if present
                if '.' in sequence_name:
                    sequence_name = sequence_name.split('.')[1]
                
                return {
                    'name': sequence_name,
                    'type': 'auto_increment'
                }
        except Exception:
            pass
        
        return None

    def extract_schema(self, output_file: str) -> None:
        """
        Extract complete schema information and save to JSON file.
        
        Args:
            output_file: Path where to save the JSON file
        """
        # Create schema metadata
        metadata = SchemaMetadata(
            name=self.schema,
            dialect=self.inspector.engine.dialect.name,
            version=str(self.inspector.engine.dialect.server_version_info),
            generated_at=datetime.now().isoformat()
        )

        # Create schema model
        schema_model = SchemaModel(
            metadata=metadata,
            tables={}
        )

        # First pass: gather basic table information
        for table_name in self.inspector.get_table_names(schema=self.schema):
            schema_model.tables[table_name] = self._get_table_details(table_name)

        # Second pass: process foreign key relationships
        self._process_foreign_keys(schema_model)

        # Third pass: analyze relationships
        for table_name, table in schema_model.tables.items():
            table.relationships = self._analyze_relationships(table, schema_model)

        # Save to file
        schema_model.save(output_file)

if __name__ == "__main__":
    from database_connector import DatabaseConnector
    from db_operations import DatabaseOperations
    
    # Connect to database
    connector = DatabaseConnector()
    if connector.connect():
        try:
            db_ops = DatabaseOperations(connector)
            extractor = SchemaExtractor(db_ops)
            
            # Extract schema to JSON
            output_file = 'schema.json'
            extractor.extract_schema(output_file)
            print(f"\nSchema extracted to {output_file}")
            
        finally:
            connector.close() 