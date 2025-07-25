"""
Column classes for table models.
Provides base functionality for different data types.
"""

import random
from datetime import datetime
from typing import Any, Callable, Optional
from faker import Faker

fake = Faker()

class Column:
    """Base column class."""
    
    def __init__(
        self,
        nullable: bool = True,
        unique: bool = False,
        generator: Optional[Callable[[], Any]] = None,
        skip_generation: bool = False
    ):
        self.nullable = nullable
        self.unique = unique
        self.generator = generator
        self.skip_generation = skip_generation
    
    def generate(self) -> Any:
        """Generate a value for this column."""
        if self.skip_generation:
            return None
            
        if self.nullable and random.random() < 0.1:  # 10% chance of NULL
            return None
        
        if self.generator:
            return self.generator()
        
        return self.default_generator()
    
    def default_generator(self) -> Any:
        """Default value generator. Override in subclasses."""
        return None


class IntegerColumn(Column):
    """Integer column type."""
    
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


class StringColumn(Column):
    """String column type."""
    
    def __init__(
        self,
        max_length: int = 50,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.max_length = max_length
    
    def default_generator(self) -> str:
        return fake.text(max_nb_chars=self.max_length)


class DateTimeColumn(Column):
    """DateTime column type."""
    
    def default_generator(self) -> datetime:
        return datetime.now()


class BooleanColumn(Column):
    """Boolean column type."""
    
    def __init__(
        self,
        true_probability: float = 0.5,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.true_probability = true_probability
    
    def default_generator(self) -> bool:
        return random.random() < self.true_probability


class EmailColumn(StringColumn):
    """Email column type with email generation."""
    
    def default_generator(self) -> str:
        return fake.email()


class NameColumn(StringColumn):
    """Name column type with name generation."""
    
    def __init__(
        self,
        name_type: str = 'full',  # 'full', 'first', or 'last'
        **kwargs
    ):
        super().__init__(**kwargs)
        self.name_type = name_type
    
    def default_generator(self) -> str:
        if self.name_type == 'first':
            return fake.first_name()
        elif self.name_type == 'last':
            return fake.last_name()
        return fake.name()


class PhoneColumn(StringColumn):
    """Phone column type with phone number generation."""
    
    def default_generator(self) -> str:
        return fake.phone_number() 