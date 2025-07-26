"""
Schema extractor that generates a comprehensive JSON representation of database schema.
"""

import json
from typing import Dict, List, Any, Optional
from sqlalchemy import inspect, text
from datetime import datetime

class SchemaExtractor:
    """Extracts detailed database schema information into JSON format."""

    def __init__(self, db_ops):
        """Initialize with database operations instance."""
        self.db_ops = db_ops
        self.inspector = inspect(self.db_ops.db.get_engine())
        self.schema = self.db_ops.db.get_schema()

    def _get_column_details(self, table_name: str, column: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed information about a column."""
        col_type = str(column['type'])
        type_params = {}

        # Extract type-specific parameters
        if 'enum' in col_type.lower():
            # Extract enum values
            start = col_type.find('(')
            end = col_type.rfind(')')
            if start != -1 and end != -1:
                enum_values = [
                    v.strip().strip("'").strip('"') 
                    for v in col_type[start + 1:end].split(',')
                ]
                type_params['enum_values'] = enum_values

        elif hasattr(column['type'], 'length'):
            type_params['length'] = column['type'].length

        elif hasattr(column['type'], 'precision'):
            type_params['precision'] = column['type'].precision
            if hasattr(column['type'], 'scale'):
                type_params['scale'] = column['type'].scale

        # Get default value
        default = column.get('default', None)
        if default is not None:
            default = str(default)

        # Check if it's auto-incrementing
        is_auto_increment = any([
            column.get('autoincrement', False),
            'serial' in col_type.lower(),
            'nextval' in str(default or '').lower(),
            column.get('identity', False)
        ])

        return {
            'name': column['name'],
            'type': {
                'python': column['type'].__class__.__name__,
                'sql': col_type,
                'params': type_params
            },
            'nullable': column.get('nullable', True),
            'default': default,
            'primary_key': column.get('primary_key', False),
            'unique': False,  # Will be updated when processing constraints
            'auto_increment': is_auto_increment,
            'comment': column.get('comment', None),
            'references': [],  # Will be filled with foreign key info
            'referenced_by': []  # Will be filled with tables referencing this column
        }

    def _get_index_details(self, table_name: str) -> List[Dict[str, Any]]:
        """Get details about table indexes."""
        indexes = []
        for idx in self.inspector.get_indexes(table_name, schema=self.schema):
            indexes.append({
                'name': idx['name'],
                'columns': idx['column_names'],
                'unique': idx['unique'],
                'type': idx.get('type', 'btree')
            })
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

    def _process_foreign_keys(self, schema_data: Dict[str, Any]) -> None:
        """Process foreign key relationships and update column references."""
        for table_name, table_info in schema_data['tables'].items():
            fks = self.inspector.get_foreign_keys(table_name, schema=self.schema)
            
            for fk in fks:
                for local_col, remote_col in zip(
                    fk['constrained_columns'],
                    fk['referred_columns']
                ):
                    # Add reference to the local column
                    for col in table_info['columns']:
                        if col['name'] == local_col:
                            col['references'].append({
                                'table': fk['referred_table'],
                                'column': remote_col,
                                'name': fk['name'],
                                'onupdate': fk.get('onupdate'),
                                'ondelete': fk.get('ondelete')
                            })
                    
                    # Add referenced_by to the remote column
                    remote_table = schema_data['tables'][fk['referred_table']]
                    for col in remote_table['columns']:
                        if col['name'] == remote_col:
                            col['referenced_by'].append({
                                'table': table_name,
                                'column': local_col,
                                'name': fk['name']
                            })

    def _get_table_details(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive details about a table."""
        columns = []
        for col in self.inspector.get_columns(table_name, schema=self.schema):
            columns.append(self._get_column_details(table_name, col))

        # Get table comment if available
        table_comment = None
        try:
            if hasattr(self.inspector, 'get_table_comment'):
                comment_info = self.inspector.get_table_comment(table_name, schema=self.schema)
                table_comment = comment_info.get('text')
        except NotImplementedError:
            pass

        return {
            'name': table_name,
            'schema': self.schema,
            'columns': columns,
            'indexes': self._get_index_details(table_name),
            'constraints': self._get_constraint_details(table_name),
            'comment': table_comment
        }

    def _get_schema_metadata(self) -> Dict[str, Any]:
        """Get metadata about the schema itself."""
        return {
            'name': self.schema,
            'dialect': self.inspector.engine.dialect.name,
            'version': str(self.inspector.engine.dialect.server_version_info),
            'generated_at': datetime.now().isoformat()
        }

    def extract_schema(self, output_file: str) -> None:
        """
        Extract complete schema information and save to JSON file.
        
        Args:
            output_file: Path where to save the JSON file
        """
        schema_data = {
            'metadata': self._get_schema_metadata(),
            'tables': {}
        }

        # First pass: gather basic table information
        for table_name in self.inspector.get_table_names(schema=self.schema):
            schema_data['tables'][table_name] = self._get_table_details(table_name)

        # Second pass: process foreign key relationships
        self._process_foreign_keys(schema_data)

        # Update unique flags on columns based on constraints
        for table_info in schema_data['tables'].values():
            # From unique constraints
            for const in table_info['constraints']['unique']:
                for col_name in const['columns']:
                    for col in table_info['columns']:
                        if col['name'] == col_name:
                            col['unique'] = True

            # From unique indexes
            for idx in table_info['indexes']:
                if idx['unique']:
                    for col_name in idx['columns']:
                        for col in table_info['columns']:
                            if col['name'] == col_name:
                                col['unique'] = True

        # Save to file with nice formatting
        with open(output_file, 'w') as f:
            json.dump(schema_data, f, indent=2)

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