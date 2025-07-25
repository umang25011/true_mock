"""
Model generator that creates Python table models from database tables.
"""

import os
from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta
import random
import inflection
from faker import Faker

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
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def _get_column_type_and_args(self, column_name: str, column_info: Dict[str, Any]) -> Tuple[str, List[str]]:
        """Determine the appropriate column type and arguments."""
        name_lower = column_name.lower()
        type_lower = str(column_info['type']).lower()
        args = []

        # Handle nullable
        if not column_info.get('nullable', True):
            args.append('nullable=False')

        # Handle unique constraint
        if column_info.get('unique', False):
            args.append('unique=True')

        # Special column name handling
        if 'id' in name_lower or 'key' in name_lower:
            return 'IntegerColumn', [
                'nullable=False',
                'min_value=1',
                'max_value=1000000',
                'generator=lambda: random.randint(1, 1000000)'
            ]
        
        elif any(name in name_lower for name in ['first_name', 'firstname']):
            return 'NameColumn', [
                'name_type="first"',
                f'max_length={column_info["type"].length if hasattr(column_info["type"], "length") else 50}',
                'generator=lambda: fake.first_name()'
            ]
        
        elif any(name in name_lower for name in ['last_name', 'lastname']):
            return 'NameColumn', [
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
    
    def generate_model_code(self, table_name: str) -> str:
        """Generate Python code for a table model."""
        schema = self.db_ops.get_table_schema(table_name)
        class_name = inflection.camelize(table_name) + 'Table'
        
        # Track needed imports
        needed_imports = {
            'datetime': ['datetime', 'timedelta'],
            'random': ['random'],
            'faker': ['Faker'],
            'core': set()
        }
        
        # Generate column definitions
        column_defs = []
        for col in schema['columns']:
            column_type, args = self._get_column_type_and_args(col['name'], col)
            needed_imports['core'].add(column_type)
            
            if args:
                column_defs.append(
                    f"            '{col['name']}': {column_type}(\n" +
                    ",\n".join(f"                {arg}" for arg in args) +
                    "\n            )"
                )
            else:
                column_defs.append(f"            '{col['name']}': {column_type}()")
        
        # Join column definitions with commas
        column_defs_str = ",\n".join(column_defs)
        
        # Generate the code
        imports = [
            '"""',
            f'Table model for {table_name} table.',
            'Generated automatically by sql_data_generator.',
            '"""',
            ''
        ]

        # Standard library imports first
        if needed_imports['datetime']:
            imports.append(f"from datetime import {', '.join(needed_imports['datetime'])}")
        if needed_imports['random']:
            imports.append('import random')
        
        # Third-party imports
        if needed_imports['faker']:
            imports.append('from faker import Faker')
            imports.append('')
            imports.append('fake = Faker()')
        
        # Local imports
        imports.extend([
            '',
            'from core import TableModel',
            'from core import (' + 
            ', '.join(sorted(needed_imports['core'])) + ')',
            '',
            '',
            f'class {class_name}(TableModel):',
            f'    """Model for {table_name} table."""',
            '',
            '    def _setup_columns(self):',
            '        """Define the table columns."""',
            '        self.columns = {',
            column_defs_str,
            '        }',
            ''
        ])
        
        return '\n'.join(imports)
    
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