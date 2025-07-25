"""
Table configuration generator for SQL data generation.
Generates and manages JSON configuration files for customizing data generation.
"""

import json
import os
from typing import Dict, Any, List, Optional
from datetime import datetime


class TableConfigGenerator:
    """Generates and manages table configurations for data generation."""

    DEFAULT_GENERATORS = {
        "integer": {
            "type": "random_int",
            "min": 1,
            "max": 1000
        },
        "bigint": {
            "type": "random_int",
            "min": 1,
            "max": 1000000
        },
        "varchar": {
            "type": "faker",
            "method": "text",
            "max_length": 50
        },
        "text": {
            "type": "faker",
            "method": "paragraph"
        },
        "timestamp": {
            "type": "datetime",
            "start_date": "2023-01-01",
            "end_date": "2024-12-31"
        },
        "date": {
            "type": "date",
            "start_date": "2023-01-01",
            "end_date": "2024-12-31"
        },
        "boolean": {
            "type": "random_choice",
            "choices": [True, False],
            "weights": [0.5, 0.5]
        },
        "email": {
            "type": "faker",
            "method": "email"
        },
        "name": {
            "type": "faker",
            "method": "name"
        },
        "phone": {
            "type": "faker",
            "method": "phone_number"
        }
    }

    def __init__(self, db_ops):
        """Initialize with database operations instance."""
        self.db_ops = db_ops
        self.output_dir = "table_configs"
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def _guess_generator_type(self, column_name: str, column_type: str) -> Dict[str, Any]:
        """Guess the appropriate generator based on column name and type."""
        name_lower = column_name.lower()
        type_lower = column_type.lower()

        # Check common column names first
        if "email" in name_lower:
            return self.DEFAULT_GENERATORS["email"]
        elif any(name in name_lower for name in ["name", "first", "last"]):
            return self.DEFAULT_GENERATORS["name"]
        elif any(name in name_lower for name in ["phone", "mobile", "tel"]):
            return self.DEFAULT_GENERATORS["phone"]
        
        # Check data types
        for type_key in self.DEFAULT_GENERATORS:
            if type_key in type_lower:
                return self.DEFAULT_GENERATORS[type_key]
        
        # Default to text if no match
        return self.DEFAULT_GENERATORS["varchar"]

    def generate_table_config(self, table_name: str) -> Dict[str, Any]:
        """Generate configuration template for a table."""
        schema = self.db_ops.get_table_schema(table_name)
        
        config = {
            "table_name": table_name,
            "schema": schema["table_name"],  # database schema name
            "generation_rules": {
                "batch_size": 1000,
                "total_rows": 10000,
                "seed": 42,
                "unique_check": True
            },
            "columns": {},
            "relationships": {
                "foreign_keys": [],
                "dependencies": []
            },
            "post_generation_rules": {
                "unique_combinations": []
            }
        }

        # Process columns
        for col in schema["columns"]:
            column_config = {
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "primary_key": False,
                "foreign_key": None,
                "unique": False,
                "generator": self._guess_generator_type(col["name"], str(col["type"]))
            }
            config["columns"][col["name"]] = column_config

        # Add primary key information
        for pk in schema["primary_keys"].get("constrained_columns", []):
            if pk in config["columns"]:
                config["columns"][pk]["primary_key"] = True
                config["columns"][pk]["generator"] = {"type": "auto_increment"}

        # Add foreign key information
        for fk in schema["foreign_keys"]:
            local_col = fk["constrained_columns"][0]
            if local_col in config["columns"]:
                fk_info = {
                    "local_column": local_col,
                    "foreign_table": fk["referred_table"],
                    "foreign_column": fk["referred_columns"][0],
                    "update_rule": fk.get("options", {}).get("onupdate", "NO ACTION"),
                    "delete_rule": fk.get("options", {}).get("ondelete", "NO ACTION")
                }
                config["relationships"]["foreign_keys"].append(fk_info)
                config["columns"][local_col]["foreign_key"] = fk_info

        return config

    def save_table_config(self, table_name: str) -> str:
        """Generate and save configuration file for a table."""
        config = self.generate_table_config(table_name)
        
        # Create filename
        filename = f"{table_name}_config.json"
        filepath = os.path.join(self.output_dir, filename)
        
        # Save with nice formatting
        with open(filepath, 'w') as f:
            json.dump(config, f, indent=2)
        
        return filepath

    def generate_all_configs(self) -> List[str]:
        """Generate configuration files for all tables."""
        tables = self.db_ops.get_tables()
        generated_files = []
        
        for table in tables:
            filepath = self.save_table_config(table)
            generated_files.append(filepath)
        
        return generated_files


if __name__ == "__main__":
    from sql_data_generator.core.database_connector import DatabaseConnector
    from sql_data_generator.core.db_operations import DatabaseOperations
    
    # Connect to database
    connector = DatabaseConnector()
    if connector.connect():
        db_ops = DatabaseOperations(connector)
        
        # Create config generator
        config_gen = TableConfigGenerator(db_ops)
        
        # Generate configs for all tables
        generated_files = config_gen.generate_all_configs()
        
        print("\nGenerated configuration files:")
        for file in generated_files:
            print(f"- {file}")
            
            # Show example of the generated config
            with open(file, 'r') as f:
                config = json.load(f)
                print(f"\nExample configuration for table {config['table_name']}:")
                print(json.dumps(config, indent=2))
                print("-" * 80)
        
        connector.close() 