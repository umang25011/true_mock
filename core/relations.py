"""
Relationship handling for table models.
Defines different types of relationships and their data generation behavior.
"""

from enum import Enum
from typing import Optional, List, Any, Dict
from dataclasses import dataclass
import random

class RelationType(Enum):
    """Types of relationships between tables."""
    ONE_TO_ONE = "OneToOne"
    ONE_TO_MANY = "OneToMany"
    MANY_TO_ONE = "ManyToOne"
    MANY_TO_MANY = "ManyToMany"

@dataclass
class RelationConfig:
    """Configuration for relationship data generation."""
    min_related: int = 1  # Minimum number of related records
    max_related: int = 5  # Maximum number of related records
    auto_generate: bool = True  # Whether to auto-generate related data
    cache_existing: bool = True  # Whether to cache existing related data
    pool_size: int = 10  # Size of the pool for related data generation

class Relation:
    """Base class for all relations."""
    
    def __init__(
        self,
        from_table: str,
        to_table: str,
        from_column: str,
        to_column: str,
        config: Optional[RelationConfig] = None
    ):
        self.from_table = from_table
        self.to_table = to_table
        self.from_column = from_column
        self.to_column = to_column
        self.config = config or RelationConfig()
        self._cached_data: Dict[str, Any] = {}

    def generate_related_data(self, model_registry) -> Any:
        """Generate or fetch related data - to be implemented by subclasses."""
        raise NotImplementedError

    def clear_cache(self):
        """Clear cached related data."""
        self._cached_data.clear()

    def _ensure_cached_data(self, model_registry) -> List[Any]:
        """Ensure cached data exists, generate if needed."""
        if not self._cached_data.get(self.to_table) or not self.config.cache_existing:
            related_model = model_registry.get_model(self.to_table)
            self._cached_data[self.to_table] = [
                row[self.to_column] 
                for row in related_model.generate_rows(self.config.pool_size)
            ]
        return self._cached_data[self.to_table]

class OneToOneRelation(Relation):
    """One-to-One relationship handler."""
    
    def generate_related_data(self, model_registry) -> Any:
        """Generate exactly one related record."""
        related_model = model_registry.get_model(self.to_table)
        related_data = related_model.generate_row()
        return related_data[self.to_column]

class ManyToOneRelation(Relation):
    """Many-to-One relationship handler."""
    
    def generate_related_data(self, model_registry) -> Any:
        """Generate or get one record that can be referenced multiple times."""
        cached_data = self._ensure_cached_data(model_registry)
        return random.choice(cached_data)

class OneToManyRelation(Relation):
    """One-to-Many relationship handler."""
    
    def generate_related_data(self, model_registry) -> List[Any]:
        """Generate multiple related records."""
        count = random.randint(self.config.min_related, self.config.max_related)
        related_model = model_registry.get_model(self.to_table)
        return [
            row[self.to_column] 
            for row in related_model.generate_rows(count)
        ]

class ManyToManyRelation(Relation):
    """Many-to-Many relationship handler."""
    
    def __init__(
        self,
        from_table: str,
        to_table: str,
        junction_table: str,
        from_column: str,
        to_column: str,
        config: Optional[RelationConfig] = None
    ):
        super().__init__(from_table, to_table, from_column, to_column, config)
        self.junction_table = junction_table

    def generate_related_data(self, model_registry) -> List[Any]:
        """Generate multiple related records through junction table."""
        count = random.randint(self.config.min_related, self.config.max_related)
        cached_data = self._ensure_cached_data(model_registry)
        return random.sample(cached_data, min(count, len(cached_data)))

class ModelRegistry:
    """Registry to manage and cache table models."""
    
    def __init__(self):
        self._models: Dict[str, 'TableModel'] = {}

    def register_model(self, table_name: str, model: 'TableModel'):
        """Register a model for a table."""
        self._models[table_name] = model
        model.set_model_registry(self)

    def get_model(self, table_name: str) -> 'TableModel':
        """Get the model for a table."""
        if table_name not in self._models:
            raise KeyError(f"Model for table '{table_name}' not found in registry")
        return self._models[table_name]

    def clear_caches(self):
        """Clear all cached data in all models."""
        for model in self._models.values():
            for relation in model.relations:
                relation.clear_cache() 