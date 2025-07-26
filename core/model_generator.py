"""
Model generator that creates Python table models from database tables.
"""

import os
from typing import Dict, Any, List, Tuple, Set
from datetime import datetime, timedelta
import random
import inflection
from faker import Faker
from sqlalchemy import inspect
from jinja2 import Environment, FileSystemLoader

# Import base types first
from .columns import Column
# Then import specific column types
from .columns import (
    IntegerColumn, StringColumn, DateTimeColumn,
    EmailColumn, NameColumn, PhoneColumn, ReferenceColumn
)
# Import relation types last to avoid circular imports
from .relations import (
    RelationType, RelationConfig, Relation,
    OneToOneRelation, OneToManyRelation, ManyToOneRelation, ManyToManyRelation
)

fake = Faker()

class ModelGenerator:
    """Generates Python table models from database tables."""
    
    COLUMN_TYPE_MAPPING = {
        'integer': 'integer',
        'bigint': 'integer',
        'smallint': 'integer',
        'varchar': 'string',
        'char': 'string',
        'text': 'string',
        'boolean': 'boolean',
        'timestamp': 'datetime',
        'date': 'datetime',
        'time': 'datetime',
        'email': 'email',
        'name': 'name',
        'phone': 'phone',
        'enum': 'enum'
    }

    def __init__(self, db_ops):
        """Initialize with database operations instance."""
        self.db_ops = db_ops
        self.output_dir = "models"
        
        # Set up Jinja2 environment
        template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def _is_auto_incrementing(self, table_name: str, column_info: Dict[str, Any]) -> bool:
        """Check if a column is auto-incrementing (serial)."""
        # Get the raw column info from SQLAlchemy inspector
        inspector = inspect(self.db_ops.db.get_engine())
        raw_columns = inspector.get_columns(table_name, schema=self.db_ops.db.get_schema())
        
        # Find matching column
        for raw_col in raw_columns:
            if raw_col['name'] == column_info['name']:
                # Check for serial/auto-increment flags
                # Different databases might use different attributes
                is_serial = any([
                    raw_col.get('autoincrement', False),  # SQLAlchemy's autoincrement flag
                    'serial' in str(raw_col['type']).lower(),  # PostgreSQL serial type
                    'nextval' in str(raw_col.get('default', '')).lower(),  # PostgreSQL sequence default
                    raw_col.get('identity', False)  # SQL Server/PostgreSQL identity columns
                ])
                return is_serial
        return False

    def _render_column(self, template_name: str, context: Dict[str, Any]) -> str:
        """Render a column using its template."""
        template = self.jinja_env.get_template(f'columns/{template_name}.jinja')
        return template.render(**context)

    def _get_enum_values(self, column_info: Dict[str, Any]) -> List[str]:
        """Extract enum values from column info."""
        type_str = str(column_info['type'])
        if 'enum' not in type_str.lower():
            return []
        
        # Extract values from enum type string (e.g., "ENUM('M', 'F')" -> ["'M'", "'F'"])
        start = type_str.find('(')
        end = type_str.rfind(')')
        if start == -1 or end == -1:
            return []
        
        values = type_str[start + 1:end].split(',')
        return [v.strip() for v in values]

    def _get_column_definition(
        self,
        table_name: str,
        column_name: str,
        column_info: Dict[str, Any]
    ) -> str:
        """Get column definition using templates."""
        name_lower = column_name.lower()
        type_lower = str(column_info['type']).lower()
        context = {}

        # Handle basic attributes
        if not column_info.get('nullable', True):
            context['nullable'] = 'False'
        if column_info.get('unique', False):
            context['unique'] = 'True'

        # Check if this is a primary key and auto-incrementing
        is_primary = column_info.get('primary_key', False)
        is_auto_increment = self._is_auto_incrementing(table_name, column_info)

        # For auto-incrementing primary keys, skip value generation
        if is_primary and is_auto_increment:
            return self._render_column('integer', {
                'nullable': 'False',
                'skip_generation': 'True',
                'generator': 'None'
            })
        
        # Check if this is a foreign key
        inspector = inspect(self.db_ops.db.get_engine())
        schema = self.db_ops.db.get_schema()
        foreign_keys = inspector.get_foreign_keys(table_name, schema=schema)
        
        # Find if this column is part of a foreign key
        for fk in foreign_keys:
            if column_name in fk['constrained_columns']:
                return self._render_column('reference', {
                    'nullable': 'False',
                    'to_table': fk["referred_table"],
                    'to_column': fk["referred_columns"][0]
                })

        # Handle special column types based on name
        if any(name in name_lower for name in ['first_name', 'firstname']):
            return self._render_column('name', {
                'nullable': 'False',
                'name_type': 'first',
                'max_length': getattr(column_info['type'], 'length', 50)
            })
        
        elif any(name in name_lower for name in ['last_name', 'lastname']):
            return self._render_column('name', {
                'nullable': 'False',
                'name_type': 'last',
                'max_length': getattr(column_info['type'], 'length', 50)
            })
        
        elif 'email' in name_lower:
            return self._render_column('email', context)
        
        elif 'phone' in name_lower:
            return self._render_column('phone', context)
        
        # Handle basic types
        template_name = self.COLUMN_TYPE_MAPPING.get(
            next((t for t in self.COLUMN_TYPE_MAPPING if t in type_lower), 'string')
        )
        
        # Add type-specific context
        if template_name == 'string':
            context['max_length'] = getattr(column_info['type'], 'length', 50)
            if 'char(' in type_lower and not 'var' in type_lower:
                context['generator'] = f'lambda: "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k={context["max_length"]}))'
        
        elif template_name == 'integer':
            if 'id' in name_lower or 'key' in name_lower:
                context.update({
                    'nullable': 'False',
                    'min_value': 1,
                    'max_value': 1000000
                })
            elif 'salary' in name_lower:
                context.update({
                    'min_value': 30000,
                    'max_value': 150000
                })
            elif 'age' in name_lower:
                context.update({
                    'min_value': 18,
                    'max_value': 100
                })
            else:
                context.update({
                    'min_value': 1,
                    'max_value': 1000000
                })
        
        elif template_name == 'datetime':
            if any(term in name_lower for term in ['birth', 'dob', 'birthdate']):
                context['generator'] = 'lambda: datetime.now() - timedelta(days=random.randint(365*20, 365*60))'
            elif any(term in name_lower for term in ['hire', 'start', 'join']):
                context['generator'] = 'lambda: datetime.now() - timedelta(days=random.randint(0, 365*10))'
            elif any(term in name_lower for term in ['end', 'finish', 'complete']):
                context['generator'] = 'lambda: datetime.now() + timedelta(days=random.randint(0, 365*2))'
            else:
                context['generator'] = 'lambda: fake.date_time_this_decade()'
        
        elif template_name == 'enum':
            enum_values = self._get_enum_values(column_info)
            if enum_values:
                context['choices'] = enum_values
            elif 'gender' in name_lower:
                context['choices'] = ["'M'", "'F'"]
        
        return self._render_column(template_name, context)

    def _detect_relationship_type(
        self, 
        inspector, 
        table_name: str,
        fk_info: Dict[str, Any],
        schema: str
    ) -> RelationType:
        """Detect the type of relationship based on database constraints."""
        referred_table = fk_info['referred_table']
        
        # Get constraints for both tables
        constraints_from = inspector.get_unique_constraints(table_name, schema=schema)
        pk_from = inspector.get_pk_constraint(table_name, schema=schema)
        fks_to = inspector.get_foreign_keys(referred_table, schema=schema)
        
        # Check if the foreign key column is unique
        fk_columns = fk_info['constrained_columns']
        is_unique = any(
            set(fk_columns) == set(const['column_names'])
            for const in constraints_from
        ) or set(fk_columns) == set(pk_from.get('constrained_columns', []))
        
        # Check for reverse relationships
        has_reverse_relation = any(
            fk['referred_table'] == table_name
            for fk in fks_to
        )
        
        if is_unique:
            return RelationType.ONE_TO_ONE
        elif has_reverse_relation:
            # Check if it's many-to-many through a junction table
            if len(fk_info['constrained_columns']) > 1:
                return RelationType.MANY_TO_MANY
            else:
                return RelationType.ONE_TO_MANY
        else:
            return RelationType.MANY_TO_ONE

    def _analyze_relationships(self, table_name: str) -> List[Relation]:
        """Analyze and return all relationships for a table."""
        # We no longer need this since relationships are handled by RelationColumn
        return []

    def generate_model_code(self, table_name: str) -> str:
        """Generate Python code for a table model."""
        schema = self.db_ops.get_table_schema(table_name)
        class_name = inflection.camelize(table_name) + 'Table'
        
        # Track needed imports
        imports = {
            'datetime': set(),
            'random': False,
            'faker': False,
            'core': set()
        }
        
        # Generate column definitions
        columns = []
        for col in schema['columns']:
            column_def = self._get_column_definition(table_name, col['name'], col)
            columns.append({
                'name': col['name'],
                'definition': column_def
            })
            
            # Track imports based on column types
            if 'datetime' in column_def:
                imports['datetime'].add('datetime')
            if 'timedelta' in column_def:
                imports['datetime'].add('timedelta')
            if 'random.' in column_def:
                imports['random'] = True
            if 'fake.' in column_def:
                imports['faker'] = True
            for col_type in ['Integer', 'String', 'DateTime', 'Email', 'Name', 'Phone', 'Reference']:
                if f'{col_type}Column' in column_def:
                    imports['core'].add(f'{col_type}Column')
        
        # Render template
        template = self.jinja_env.get_template('model.py.jinja')
        return template.render(
            table_name=table_name,
            class_name=class_name,
            imports=imports,
            columns=columns
        )
    
    def save_model(self, table_name: str) -> str:
        """Generate and save a model file for a table."""
        code = self.generate_model_code(table_name)
        
        # Create filename
        filename = f"{inflection.underscore(table_name)}_table.py"
        filepath = os.path.join(self.output_dir, filename)
        
        # Save the file
        with open(filepath, 'w') as f:
            f.write(code)
        
        return filepath
    
    def generate_all_models(self) -> List[str]:
        """Generate model files for all tables."""
        tables = self.db_ops.get_tables()
        generated_files = []
        
        for table in tables:
            filepath = self.save_model(table)
            generated_files.append(filepath)
        
        # Generate __init__.py to expose all models
        init_path = os.path.join(self.output_dir, '__init__.py')
        with open(init_path, 'w') as f:
            f.write('"""Generated table models."""\n\n')
            for table in tables:
                module_name = f"{inflection.underscore(table)}_table"
                class_name = f"{inflection.camelize(table)}Table"
                f.write(f'from .{module_name} import {class_name}\n')
            
            f.write('\n__all__ = [\n    ')
            f.write(',\n    '.join(f"'{inflection.camelize(table)}Table'" for table in tables))
            f.write('\n]\n')
        
        return generated_files


if __name__ == "__main__":
    # Example usage
    from sql_data_generator.core import DatabaseConnector, DatabaseOperations
    
    # Connect to database
    connector = DatabaseConnector()
    if connector.connect():
        db_ops = DatabaseOperations(connector)
        
        # Create model generator
        generator = ModelGenerator(db_ops)
        
        # Generate models for all tables
        generated_files = generator.generate_all_models()
        
        print("\nGenerated model files:")
        for file in generated_files:
            print(f"\n- {file}")
            with open(file, 'r') as f:
                print("\nContent:")
                print(f.read())
            print("-" * 80)
        
        connector.close()