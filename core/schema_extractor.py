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
        if 'enum' in col_type.lower() or (
            # PostgreSQL specific enum detection
            hasattr(column['type'], 'enums') and 
            getattr(column['type'], 'enums', None)
        ):
            # Try PostgreSQL enum values first
            enum_values = getattr(column['type'], 'enums', None)
            if not enum_values:
                # Fall back to parsing type string
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

        # Get default value and sequence info
        default = column.get('default', None)
        sequence_info = None
        if default is not None:
            default = str(default)
            sequence_info = self._get_sequence_info(column)

        # Check if it's auto-incrementing
        is_auto_increment = any([
            column.get('autoincrement', False),
            'serial' in col_type.lower(),
            sequence_info is not None,
            column.get('identity', False)
        ])

        # Determine the Python type name
        python_type = column['type'].__class__.__name__
        if hasattr(column['type'], 'enums'):
            python_type = 'ENUM'  # Override for PostgreSQL enums

        return {
            'name': column['name'],
            'type': {
                'python': python_type,
                'sql': col_type,
                'params': type_params
            },
            'nullable': column.get('nullable', True),
            'default': default,
            'sequence': sequence_info,
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
                                'onupdate': fk.get('onupdate', 'NO ACTION'),
                                'ondelete': fk.get('ondelete', 'NO ACTION')
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

    def _analyze_relationships(self, table_info: Dict[str, Any], schema_data: Dict[str, Any]) -> Dict[str, List[str]]:
        """Analyze table relationships based on foreign keys."""
        relationships = {
            'has_many': [],
            'belongs_to': [],
            'has_one': [],
            'many_to_many': []  # Added for explicit M:N relationships
        }
        
        # Check each column's references and referenced_by
        for col in table_info['columns']:
            # belongs_to relationships (foreign keys)
            for ref in col['references']:
                relationships['belongs_to'].append(ref['table'])
                
                # Check if this is part of a composite primary key
                is_composite_pk = any(
                    ck['type'] == 'primary_key' and len(ck['columns']) > 1
                    for ck in col['usage']['composite_keys']
                )
                if is_composite_pk:
                    if ref['table'] not in relationships['many_to_many']:
                        relationships['many_to_many'].append(ref['table'])
                    if ref['table'] in relationships['belongs_to']:
                        relationships['belongs_to'].remove(ref['table'])
            
            # has_many/has_one relationships (referenced by others)
            for ref in col['referenced_by']:
                # Check if the foreign key is part of a composite key in the referencing table
                ref_table = schema_data['tables'][ref['table']]
                pk_constraint = ref_table['constraints']['primary_key']
                is_composite = (
                    pk_constraint and 
                    len(pk_constraint['columns']) > 1 and 
                    ref['column'] in pk_constraint['columns']
                )
                
                if is_composite:
                    if ref['table'] not in relationships['many_to_many']:
                        relationships['many_to_many'].append(ref['table'])
                else:
                    # Check if the foreign key is part of a unique constraint
                    is_unique = any(
                        ref['column'] in const['columns'] 
                        for const in ref_table['constraints']['unique']
                    ) or any(
                        ref['column'] in idx['columns'] and idx['unique']
                        for idx in ref_table['indexes']
                    )
                    
                    if is_unique:
                        relationships['has_one'].append(ref['table'])
                    elif ref['table'] not in relationships['many_to_many']:
                        relationships['has_many'].append(ref['table'])
        
        # Remove duplicates while preserving order
        for key in relationships:
            relationships[key] = list(dict.fromkeys(relationships[key]))
        
        return relationships

    def _analyze_column_usage(self, column: Dict[str, Any], table_info: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze how a column is used in the table."""
        usage = {
            'is_foreign_key': bool(column['references']),
            'is_referenced': bool(column['referenced_by']),
            'reference_count': len(column['referenced_by']),
            'composite_keys': []
        }

        # Check if part of composite primary key
        if table_info['constraints']['primary_key']:
            pk_columns = table_info['constraints']['primary_key']['columns']
            if len(pk_columns) > 1 and column['name'] in pk_columns:
                usage['composite_keys'].append({
                    'type': 'primary_key',
                    'name': table_info['constraints']['primary_key']['name'],
                    'columns': pk_columns,
                    'position': pk_columns.index(column['name']) + 1
                })

        # Check if part of composite unique constraints
        for const in table_info['constraints']['unique']:
            if len(const['columns']) > 1 and column['name'] in const['columns']:
                usage['composite_keys'].append({
                    'type': 'unique',
                    'name': const['name'],
                    'columns': const['columns'],
                    'position': const['columns'].index(column['name']) + 1
                })

        # Check if part of composite unique indexes
        for idx in table_info['indexes']:
            if idx['unique'] and len(idx['columns']) > 1 and column['name'] in idx['columns']:
                usage['composite_keys'].append({
                    'type': 'unique_index',
                    'name': idx['name'],
                    'columns': idx['columns'],
                    'position': idx['columns'].index(column['name']) + 1
                })

        return usage

    def _analyze_index_usage(self, index: Dict[str, Any], table_info: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze how an index is used."""
        return {
            'is_primary': (
                table_info['constraints']['primary_key'] and
                table_info['constraints']['primary_key']['name'] == index['name']
            ),
            'is_unique': index['unique'],
            'is_composite': len(index['columns']) > 1,
            'column_count': len(index['columns'])
        }

    def _get_table_details(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive details about a table."""
        table_info = {
            'name': table_name,
            'schema': self.schema,
            'columns': [],
            'indexes': [],
            'constraints': self._get_constraint_details(table_name),
            'comment': None,
            'relationships': None  # Will be filled later
        }

        # Get columns
        for col in self.inspector.get_columns(table_name, schema=self.schema):
            column_info = self._get_column_details(table_name, col)
            table_info['columns'].append(column_info)

        # Get indexes with usage analysis
        indexes = self._get_index_details(table_name)
        table_info['indexes'] = [
            {**idx, 'usage': self._analyze_index_usage(idx, table_info)}
            for idx in indexes
        ]

        # Get table comment if available
        try:
            if hasattr(self.inspector, 'get_table_comment'):
                comment_info = self.inspector.get_table_comment(table_name, schema=self.schema)
                table_info['comment'] = comment_info.get('text')
        except NotImplementedError:
            pass

        return table_info

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
        schema_data = {
            'metadata': self._get_schema_metadata(),
            'tables': {}
        }

        # First pass: gather basic table information
        for table_name in self.inspector.get_table_names(schema=self.schema):
            schema_data['tables'][table_name] = self._get_table_details(table_name)

        # Second pass: process foreign key relationships
        self._process_foreign_keys(schema_data)

        # Third pass: analyze column usage and update flags
        for table_name, table_info in schema_data['tables'].items():
            # Update column information
            for col in table_info['columns']:
                # Add usage analysis
                col['usage'] = self._analyze_column_usage(col, table_info)
                
                # Update flags based on constraints
                if table_info['constraints']['primary_key']:
                    pk_columns = set(table_info['constraints']['primary_key']['columns'])
                    if col['name'] in pk_columns:
                        col['primary_key'] = True
                        col['nullable'] = False

                # Update unique flags
                for const in table_info['constraints']['unique']:
                    if col['name'] in const['columns']:
                        col['unique'] = True

                for idx in table_info['indexes']:
                    if idx['unique'] and len(idx['columns']) == 1 and col['name'] == idx['columns'][0]:
                        col['unique'] = True

        # Fourth pass: analyze relationships
        for table_name, table_info in schema_data['tables'].items():
            table_info['relationships'] = self._analyze_relationships(table_info, schema_data)

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