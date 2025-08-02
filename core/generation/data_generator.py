"""
Data generator that uses JSON configuration files to generate data.
"""

import json
import random
from typing import Dict, Any, List
from datetime import datetime, timedelta
from faker import Faker
from dateutil.parser import parse

fake = Faker()

class DataGenerator:
    """Generates data based on JSON configuration files."""
    
    def __init__(self, config_file: str):
        """Initialize with a configuration file."""
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.table_name = self.config["table_name"]
        self.seed = self.config["generation_rules"]["seed"]
        random.seed(self.seed)
        fake.seed_instance(self.seed)
    
    def _generate_value(self, generator_config: Dict[str, Any]) -> Any:
        """Generate a value based on generator configuration."""
        gen_type = generator_config["type"]
        
        if gen_type == "auto_increment":
            return None  # Let database handle this
        
        elif gen_type == "random_int":
            return random.randint(generator_config["min"], generator_config["max"])
        
        elif gen_type == "random_choice":
            weights = generator_config.get("weights")
            if weights:
                return random.choices(generator_config["choices"], weights=weights)[0]
            return random.choice(generator_config["choices"])
        
        elif gen_type == "faker":
            method = generator_config["method"]
            faker_method = getattr(fake, method)
            if method == "text":
                return faker_method(max_nb_chars=generator_config.get("max_length", 50))
            return faker_method()
        
        elif gen_type == "datetime":
            start = parse(generator_config["start_date"])
            end = parse(generator_config["end_date"])
            delta = end - start
            random_days = random.randint(0, delta.days)
            random_seconds = random.randint(0, 24*60*60)
            return start + timedelta(days=random_days, seconds=random_seconds)
        
        elif gen_type == "date":
            start = parse(generator_config["start_date"])
            end = parse(generator_config["end_date"])
            delta = end - start
            random_days = random.randint(0, delta.days)
            return (start + timedelta(days=random_days)).date()
        
        elif gen_type == "custom":
            # For custom generators defined in the config
            if "values" in generator_config:
                return random.choice(generator_config["values"])
            elif "range" in generator_config:
                range_config = generator_config["range"]
                return random.uniform(range_config["min"], range_config["max"])
        
        return None
    
    def generate_row(self) -> Dict[str, Any]:
        """Generate a single row of data."""
        row = {}
        
        for col_name, col_config in self.config["columns"].items():
            # Skip primary keys and handle them separately
            if col_config["primary_key"]:
                continue
                
            # Handle nullable fields
            if col_config["nullable"] and random.random() < 0.1:  # 10% chance of NULL
                row[col_name] = None
                continue
            
            # Generate value based on generator configuration
            row[col_name] = self._generate_value(col_config["generator"])
        
        return row
    
    def generate_rows(self, count: int = None) -> List[Dict[str, Any]]:
        """Generate multiple rows of data."""
        if count is None:
            count = self.config["generation_rules"]["batch_size"]
        
        return [self.generate_row() for _ in range(count)]
    
    def get_foreign_key_info(self) -> List[Dict[str, Any]]:
        """Get foreign key relationships."""
        return self.config["relationships"]["foreign_keys"]
    
    def get_unique_constraints(self) -> List[List[str]]:
        """Get unique column combinations."""
        return self.config["post_generation_rules"]["unique_combinations"]


if __name__ == "__main__":
    import os
    
    # Example usage
    config_dir = "table_configs"
    
    # List all config files
    config_files = [f for f in os.listdir(config_dir) if f.endswith("_config.json")]
    
    for config_file in config_files:
        print(f"\nGenerating data for {config_file}")
        generator = DataGenerator(os.path.join(config_dir, config_file))
        
        # Generate sample data
        sample_data = generator.generate_rows(5)
        
        print(f"\nSample data for table {generator.table_name}:")
        for row in sample_data:
            print(row)
        print("-" * 80) 