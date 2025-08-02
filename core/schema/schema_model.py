"""
Schema model classes to represent database schema in a strongly-typed way.
Used for both reading and writing schema JSON files.
"""

from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

@dataclass
class ColumnType:
    """Represents a column's type information"""
    python: str
    sql: str
    sqlalchemy_type: str
    python_type: str
    params: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnType':
        return cls(
            python=data['python'],
            sql=data['sql'],
            sqlalchemy_type=data['sqlalchemy_type'],
            python_type=data['python_type'],
            params=data.get('params')
        )

@dataclass
class ColumnFlags:
    """Represents column flags and properties"""
    nullable: bool = False
    primary_key: bool = False
    auto_increment: bool = False
    unique: bool = False
    foreign_key: bool = False
    is_referenced: bool = False
    composite_primary_key: bool = False
    composite_position: Optional[int] = None
    composite_unique: Optional[List[Dict[str, Any]]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnFlags':
        return cls(
            nullable=data.get('nullable', False),
            primary_key=data.get('primary_key', False),
            auto_increment=data.get('auto_increment', False),
            unique=data.get('unique', False),
            foreign_key=data.get('foreign_key', False),
            is_referenced=data.get('is_referenced', False),
            composite_primary_key=data.get('composite_primary_key', False),
            composite_position=data.get('composite_position'),
            composite_unique=data.get('composite_unique')
        )

@dataclass
class ForeignKey:
    """Represents a foreign key reference"""
    table: str
    column: str
    name: str
    onupdate: Optional[str] = None
    ondelete: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ForeignKey':
        return cls(
            table=data['table'],
            column=data['column'],
            name=data['name'],
            onupdate=data.get('onupdate'),
            ondelete=data.get('ondelete')
        )

@dataclass
class Reference:
    """Represents a reference from another table"""
    table: str
    column: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Reference':
        return cls(
            table=data['table'],
            column=data['column']
        )

@dataclass
class Column:
    """Represents a database column"""
    name: str
    type: ColumnType
    flags: ColumnFlags
    foreign_key: Optional[ForeignKey] = None
    referenced_by: Optional[List[Reference]] = None
    default: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Column':
        return cls(
            name=data['name'],
            type=ColumnType.from_dict(data['type']),
            flags=ColumnFlags.from_dict(data['flags']),
            foreign_key=ForeignKey.from_dict(data['foreign_key']) if 'foreign_key' in data else None,
            referenced_by=[Reference.from_dict(ref) for ref in data['referenced_by']] if 'referenced_by' in data else None,
            default=data.get('default')
        )

@dataclass
class Index:
    """Represents a database index"""
    name: str
    columns: List[str]
    unique: bool
    type: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Index':
        return cls(
            name=data['name'],
            columns=data['columns'],
            unique=data['unique'],
            type=data['type']
        )

@dataclass
class ModelConfig:
    """Configuration for model generation"""
    table_name: str
    schema: str
    primary_keys: List[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelConfig':
        return cls(
            table_name=data['table_name'],
            schema=data['schema'],
            primary_keys=data['primary_keys']
        )

@dataclass
class Relationship:
    """Represents a relationship between tables"""
    type: str  # 'belongs_to', 'has_many', 'has_one'
    target: str
    foreign_key: str
    local_field: str
    back_populates: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Relationship':
        return cls(
            type=data['type'],
            target=data['target'],
            foreign_key=data['foreign_key'],
            local_field=data['local_field'],
            back_populates=data['back_populates']
        )

@dataclass
class Table:
    """Represents a database table"""
    name: str
    schema: str
    columns: List[Column]
    model_config: ModelConfig
    relationships: List[Relationship]
    indexes: Optional[List[Index]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Table':
        return cls(
            name=data['name'],
            schema=data['schema'],
            columns=[Column.from_dict(col) for col in data['columns']],
            model_config=ModelConfig.from_dict(data['model_config']),
            relationships=[Relationship.from_dict(rel) for rel in data['relationships']],
            indexes=[Index.from_dict(idx) for idx in data['indexes']] if 'indexes' in data else None
        )

@dataclass
class SchemaMetadata:
    """Metadata about the schema"""
    name: str
    dialect: str
    version: str
    generated_at: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SchemaMetadata':
        return cls(
            name=data['name'],
            dialect=data['dialect'],
            version=data['version'],
            generated_at=data['generated_at']
        )

@dataclass
class SchemaModel:
    """Root schema model class"""
    metadata: SchemaMetadata
    tables: Dict[str, Table]

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> 'SchemaModel':
        """Create SchemaModel from JSON dictionary"""
        return cls(
            metadata=SchemaMetadata.from_dict(json_data['metadata']),
            tables={
                name: Table.from_dict(table_data)
                for name, table_data in json_data['tables'].items()
            }
        )

    @classmethod
    def load(cls, file_path: str) -> 'SchemaModel':
        """Load SchemaModel from JSON file"""
        with open(file_path) as f:
            return cls.from_json(json.load(f))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for JSON serialization"""
        return asdict(self)

    def save(self, file_path: str) -> None:
        """Save schema to JSON file"""
        with open(file_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)

    def get_table(self, name: str) -> Table:
        """Get a table by name"""
        if name not in self.tables:
            raise KeyError(f"Table '{name}' not found in schema")
        return self.tables[name]

    def get_foreign_key_tables(self, table_name: str) -> List[str]:
        """Get list of tables that this table references via foreign keys"""
        table = self.get_table(table_name)
        return [
            col.foreign_key.table
            for col in table.columns
            if col.foreign_key is not None
        ]

    def get_dependent_tables(self, table_name: str) -> List[str]:
        """Get list of tables that reference this table"""
        table = self.get_table(table_name)
        dependent_tables = []
        for col in table.columns:
            if col.referenced_by:
                dependent_tables.extend(ref.table for ref in col.referenced_by)
        return list(set(dependent_tables))  # Remove duplicates

    def validate_relationships(self) -> List[str]:
        """Validate relationships between tables"""
        errors = []
        for table_name, table in self.tables.items():
            for rel in table.relationships:
                # Check target table exists
                if rel.target not in self.tables:
                    errors.append(
                        f"Table '{table_name}' has relationship to non-existent table '{rel.target}'"
                    )
                # Check back-reference exists
                target = self.tables.get(rel.target)
                if target:
                    back_refs = [r for r in target.relationships if r.target == table_name]
                    if not back_refs:
                        errors.append(
                            f"Table '{table_name}' has relationship to '{rel.target}' but no back-reference exists"
                        )
        return errors 