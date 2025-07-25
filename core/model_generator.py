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

from .relations import (
    RelationType, RelationConfig, Relation,
    OneToOneRelation, OneToManyRelation, ManyToOneRelation, ManyToManyRelation
)
from .columns import Column

fake = Faker()

class ModelGenerator:
    """Generates Python table models from database tables."""
    
    COLUMN_TYPE_MAPPING = {
        'integer': 'IntegerColumn',
        'bigint': 'IntegerColumn',
        'smallint': 'IntegerColumn',
        'varchar': 'StringColumn',
        'char': 'StringColumn',
        'text': 'StringColumn',
        'boolean': 'BooleanColumn',
        'timestamp': 'DateTimeColumn',
        'date': 'DateTimeColumn',
        'time': 'DateTimeColumn',
        'email': 'EmailColumn',
        'name': 'NameColumn',
        'phone': 'PhoneColumn'
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

    def _get_column_type_and_args(
        self,
        table_name: str,
        column_name: str,
        column_info: Dict[str, Any]
    ) -> Tuple[str, List[str]]:
        """Get column type and arguments for template."""
        name_lower = column_name.lower()
        type_lower = str(column_info['type']).lower()
        args = []

        # Handle nullable
        if not column_info.get('nullable', True):
            args.append('nullable=False')

        # Handle unique constraint
        if column_info.get('unique', False):
            args.append('unique=True')

        # Check if this is a primary key and auto-incrementing
        is_primary = column_info.get('primary_key', False)
        is_auto_increment = self._is_auto_incrementing(table_name, column_info)

        # For auto-incrementing primary keys, skip value generation
        if is_primary and is_auto_increment:
            return 'IntegerColumn', [
                'nullable=False',
                'skip_generation=True',  # New flag to skip value generation
                'generator=None'  # No generator needed
            ]
        
        # Handle other primary keys or ID columns that aren't auto-incrementing
        elif 'id' in name_lower or 'key' in name_lower:
            return 'IntegerColumn', [
                'nullable=False',
                'min_value=1',
                'max_value=1000000',
                'generator=lambda: random.randint(1, 1000000)'
            ]
        
        elif any(name in name_lower for name in ['first_name', 'firstname']):
            return 'NameColumn', [
                'nullable=False',  # Names should never be null
                'name_type="first"',
                f'max_length={column_info["type"].length if hasattr(column_info["type"], "length") else 50}',
                'generator=lambda: fake.first_name()'
            ]
        
        elif any(name in name_lower for name in ['last_name', 'lastname']):
            return 'NameColumn', [
                'nullable=False',  # Names should never be null
                'name_type="last"',
                f'max_length={column_info["type"].length if hasattr(column_info["type"], "length") else 50}',
                'generator=lambda: fake.last_name()'
            ]
        
        elif 'email' in name_lower:
            return 'EmailColumn', [
                *args,
                'generator=lambda: fake.email()'
            ]
        
        elif 'phone' in name_lower:
            return 'PhoneColumn', [
                *args,
                'generator=lambda: fake.phone_number()'
            ]
        
        elif 'gender' in name_lower:
            return 'StringColumn', [
                *args,
                'max_length=1',
                'generator=lambda: random.choice(["M", "F"])'
            ]
        
        elif any(date_term in name_lower for date_term in ['birth', 'dob', 'birthdate']):
            return 'DateTimeColumn', [
                *args,
                'generator=lambda: datetime.now() - timedelta(days=random.randint(365*20, 365*60))'
            ]
        
        elif any(date_term in name_lower for date_term in ['hire_date', 'start_date', 'joined']):
            return 'DateTimeColumn', [
                *args,
                'generator=lambda: datetime.now() - timedelta(days=random.randint(0, 365*10))'
            ]
        
        elif 'salary' in name_lower:
            return 'IntegerColumn', [
                *args,
                'min_value=30000',
                'max_value=150000',
                'generator=lambda: random.randint(30000, 150000)'
            ]
        
        elif 'age' in name_lower:
            return 'IntegerColumn', [
                *args,
                'min_value=18',
                'max_value=100',
                'generator=lambda: random.randint(18, 100)'
            ]
        
        elif 'description' in name_lower or 'comment' in name_lower:
            return 'StringColumn', [
                *args,
                'max_length=500',
                'generator=lambda: fake.text(max_nb_chars=500)'
            ]
        
        elif 'url' in name_lower or 'website' in name_lower:
            return 'StringColumn', [
                *args,
                'generator=lambda: fake.url()'
            ]
        
        elif 'address' in name_lower:
            return 'StringColumn', [
                *args,
                'generator=lambda: fake.address()'
            ]
        
        elif 'city' in name_lower:
            return 'StringColumn', [
                *args,
                'generator=lambda: fake.city()'
            ]
        
        elif 'country' in name_lower:
            return 'StringColumn', [
                *args,
                'generator=lambda: fake.country()'
            ]
        
        elif 'postal' in name_lower or 'zip' in name_lower:
            return 'StringColumn', [
                *args,
                'generator=lambda: fake.postcode()'
            ]
        
        # Handle basic types
        for db_type, column_type in self.COLUMN_TYPE_MAPPING.items():
            if db_type in type_lower:
                if 'char' in db_type:
                    max_length = column_info["type"].length if hasattr(column_info["type"], "length") else 50
                    args.extend([
                        f'max_length={max_length}',
                        f'generator=lambda: fake.text(max_nb_chars={max_length})'
                    ])
                elif 'int' in db_type:
                    args.extend([
                        'min_value=1',
                        'max_value=1000000',
                        'generator=lambda: random.randint(1, 1000000)'
                    ])
                elif 'bool' in db_type:
                    args.append('generator=lambda: random.choice([True, False])')
                elif 'date' in db_type or 'time' in db_type:
                    args.append('generator=lambda: fake.date_time_this_decade()')
                return column_type, args
        
        # Default to string with text generator
        return 'StringColumn', [
            *args,
            'generator=lambda: fake.text(max_nb_chars=50)'
        ]

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
        relationships = []
        inspector = inspect(self.db_ops.db.get_engine())
        schema = self.db_ops.db.get_schema()
        
        foreign_keys = inspector.get_foreign_keys(table_name, schema=schema)
        
        for fk in foreign_keys:
            rel_type = self._detect_relationship_type(inspector, table_name, fk, schema)
            config = RelationConfig()  # Use default config with auto_generate=True
            
            # Create appropriate relation instance based on type
            if rel_type == RelationType.ONE_TO_ONE:
                relation = OneToOneRelation(
                    from_table=table_name,
                    to_table=fk['referred_table'],
                    from_column=fk['constrained_columns'][0],
                    to_column=fk['referred_columns'][0],
                    config=config
                )
            elif rel_type == RelationType.MANY_TO_ONE:
                relation = ManyToOneRelation(
                    from_table=table_name,
                    to_table=fk['referred_table'],
                    from_column=fk['constrained_columns'][0],
                    to_column=fk['referred_columns'][0],
                    config=config
                )
            elif rel_type == RelationType.ONE_TO_MANY:
                relation = OneToManyRelation(
                    from_table=table_name,
                    to_table=fk['referred_table'],
                    from_column=fk['constrained_columns'][0],
                    to_column=fk['referred_columns'][0],
                    config=config
                )
            elif rel_type == RelationType.MANY_TO_MANY:
                relation = ManyToManyRelation(
                    from_table=table_name,
                    to_table=fk['referred_table'],
                    junction_table=table_name,  # Current table is the junction
                    from_column=fk['constrained_columns'][0],
                    to_column=fk['referred_columns'][0],
                    config=config
                )
            
            relationships.append(relation)
        
        return relationships

    def _get_column_definition(
        self,
        table_name: str,
        column_name: str,
        column_info: Dict[str, Any]
    ) -> Column:
        """Get column definition for template."""
        column_type, args = self._get_column_type_and_args(table_name, column_name, column_info)
        return Column(
            name=column_name,
            type=column_type,
            args=args
        )

    def generate_model_code(self, table_name: str) -> str:
        """Generate Python code for a table model."""
        schema = self.db_ops.get_table_schema(table_name)
        relationships = self._analyze_relationships(table_name)
        class_name = inflection.camelize(table_name) + 'Table'
        
        # Track needed imports
        imports = {
            'datetime': set(),
            'random': False,
            'faker': False,
            'core': set(),
            'relations': set()
        }
        
        # Generate column definitions
        columns = []
        for col in schema['columns']:
            column_type, args = self._get_column_type_and_args(table_name, col['name'], col)
            columns.append({
                'name': col['name'],
                'type': column_type,
                'args': args
            })
            
            # Track imports based on column types and generators
            imports['core'].add(column_type)
            
            # Check for specific imports needed
            if any('datetime' in arg or 'timedelta' in arg for arg in args):
                imports['datetime'].update(['datetime', 'timedelta'])
            if any('random.' in arg for arg in args):
                imports['random'] = True
            if any('fake.' in arg for arg in args):
                imports['faker'] = True
        
        # Add relation imports if needed
        if relationships:
            imports['relations'].update(['Relation', 'RelationConfig'])
            for rel in relationships:
                imports['relations'].add(rel.__class__.__name__)
        
        # Render template
        template = self.jinja_env.get_template('model.py.jinja')
        return template.render(
            table_name=table_name,
            class_name=class_name,
            imports=imports,
            columns=columns,
            relations=relationships
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