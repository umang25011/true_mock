"""
Column classes for table models.
Each class represents a type of database column and knows how to generate appropriate data.
"""

import random
from datetime import datetime
from typing import Any, Callable, Optional, Set
from sqlalchemy import text
from faker import Faker

fake = Faker()

class Column:
    """Base column class that all other column types inherit from."""
    
    def __init__(
        self,
        nullable: bool = True,
        unique: bool = False,
        generator: Optional[Callable[[], Any]] = None
    ):
        """
        Initialize a column.
        
        Args:
            nullable: Whether the column can contain NULL values
            unique: Whether values must be unique
            generator: Optional custom function to generate values
        """
        self.nullable = nullable
        self.unique = unique
        self.generator = generator
    
    def generate(self) -> Any:
        """Generate a value for this column."""
        # Handle NULL values if allowed
        if self.nullable and random.random() < 0.1:  # 10% chance of NULL
            return None
        
        # Use custom generator if provided
        if self.generator:
            return self.generator()
        
        # Fall back to default generator
        return self.default_generator()
    
    def default_generator(self) -> Any:
        """Default value generator. Override in subclasses."""
        return None


class IntegerColumn(Column):
    """Integer column that generates numbers within a range."""
    
    def __init__(
        self,
        min_value: int = 1,
        max_value: int = 1000,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.min_value = min_value
        self.max_value = max_value
    
    def default_generator(self) -> int:
        return random.randint(self.min_value, self.max_value)


class BooleanColumn(Column):
    """Boolean column that generates True/False values."""
    
    def default_generator(self) -> bool:
        return random.choice([True, False])


class ReferenceColumn(IntegerColumn):
    """Foreign key column that ensures referential integrity."""
    
    def __init__(
        self,
        to_table: str,
        to_column: str = 'id',
        db_connector = None,
        **kwargs
    ):
        """
        Initialize a foreign key column.
        
        Args:
            to_table: Name of the referenced table
            to_column: Name of the referenced column (usually 'id')
            db_connector: Database connector for fetching/inserting referenced values
        """
        super().__init__(**kwargs)
        self.to_table = to_table
        self.to_column = to_column
        self.db_connector = db_connector
        self._cached_values: Set[Any] = set()
    
    def _get_existing_values(self) -> None:
        """Get existing values from the referenced table."""
        if not self._cached_values and self.db_connector:
            schema = self.db_connector.get_schema()
            query = text(f"SELECT {self.to_column} FROM {schema}.{self.to_table} LIMIT 100")
            
            with self.db_connector.get_engine().connect() as conn:
                result = conn.execute(query)
                self._cached_values.update(row[0] for row in result)
    
    def _create_referenced_row(self) -> Any:
        """Create a new row in the referenced table if needed."""
        # Import here to avoid circular imports
        from .model_generator import ModelGenerator
        from .db_operations import DatabaseOperations
        
        # Generate a row in the referenced table
        db_ops = DatabaseOperations(self.db_connector)
        generator = ModelGenerator(db_ops)
        model_code = generator.generate_model_code(self.to_table)
        
        # Create and execute the model
        namespace = {}
        exec(model_code, namespace)
        model_class = next(cls for name, cls in namespace.items() if name.endswith('Table'))
        model = model_class()
        row = model.generate_row()
        
        # Insert the row and get its ID
        schema = self.db_connector.get_schema()
        columns = ', '.join(row.keys())
        values = ', '.join(f':{k}' for k in row.keys())
        query = text(f"""
            INSERT INTO {schema}.{self.to_table} ({columns})
            VALUES ({values})
            RETURNING {self.to_column}
        """)
        
        with self.db_connector.get_engine().connect() as conn:
            with conn.begin():
                result = conn.execute(query, row)
                value = result.scalar()
                self._cached_values.add(value)
                return value
    
    def default_generator(self) -> Any:
        """Generate a valid foreign key value."""
        # Try to get existing values first
        self._get_existing_values()
        
        # If no values exist, create a new referenced row
        if not self._cached_values:
            return self._create_referenced_row()
        
        # Return a random existing value
        return random.choice(list(self._cached_values))


class StringColumn(Column):
    """String column that generates text of a specific length."""
    
    def __init__(
        self,
        max_length: int = 50,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.max_length = max_length
    
    def default_generator(self) -> str:
        # For very short strings, generate uppercase codes
        if self.max_length <= 4:
            return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=self.max_length))
        # For longer strings, use faker
        return fake.text(max_nb_chars=self.max_length)


class DateTimeColumn(Column):
    """DateTime column that generates timestamps."""
    
    def default_generator(self) -> datetime:
        return fake.date_time_this_decade()


class EmailColumn(StringColumn):
    """Email column that generates valid email addresses."""
    
    def default_generator(self) -> str:
        return fake.email()


class NameColumn(StringColumn):
    """Name column that generates realistic names."""
    
    def __init__(
        self,
        name_type: str = 'full',  # 'full', 'first', or 'last'
        **kwargs
    ):
        super().__init__(**kwargs)
        self.name_type = name_type
    
    def default_generator(self) -> str:
        generators = {
            'first': fake.first_name,
            'last': fake.last_name,
            'full': fake.name
        }
        return generators.get(self.name_type, fake.name)()


class PhoneColumn(StringColumn):
    """Phone column that generates phone numbers."""
    
    def default_generator(self) -> str:
        return fake.phone_number() 