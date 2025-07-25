"""
Main example script demonstrating usage of the SQL data generator.
"""

import os
import sys
import importlib.util

# Add parent directory to Python path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.database_connector import DatabaseConnector
from core.db_operations import DatabaseOperations
from core.model_generator import ModelGenerator


def generate_and_test_model(generator: ModelGenerator, table_name: str) -> None:
    """Generate and test a model for a specific table."""
    print(f"\nGenerating model for table: {table_name}")
    print("=" * 50)
    
    # Generate and save the model
    model_file = generator.save_model(table_name)
    
    # Show the generated code
    print(f"\nGenerated model file: {model_file}")
    print("\nGenerated code:")
    print("=" * 50)
    with open(model_file, 'r') as f:
        print(f.read())
    
    # Import and use the generated model
    print("\nTesting the generated model:")
    print("=" * 50)
    
    # Dynamic import of the generated model
    module_name = f"{table_name}_table"
    class_name = ''.join(word.capitalize() for word in table_name.split('_')) + 'Table'
    
    spec = importlib.util.spec_from_file_location(module_name, model_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    # Create instance of the generated model
    TableClass = getattr(module, class_name)
    table_model = TableClass()
    
    # Show table structure
    print("\nTable Structure:")
    print(table_model)
    
    # Generate some sample data
    print("\nSample Generated Data:")
    sample_data = table_model.generate_rows(3)
    for row in sample_data:
        print("\nRow:")
        for field, value in row.items():
            print(f"  {field}: {value}")


def run_examples():
    """Run all examples."""
    print("SQL Data Generator Examples")
    print("=" * 50)
    
    # Connect to database
    connector = DatabaseConnector()
    if connector.connect():
        try:
            db_ops = DatabaseOperations(connector)
            
            # Get available tables
            tables = db_ops.get_tables()
            print("\nAvailable tables:")
            for i, table in enumerate(tables, 1):
                print(f"{i}. {table}")
            
            if not tables:
                print("No tables found in database!")
                return
            
            # Create model generator
            generator = ModelGenerator(db_ops)
            
            # Generate and test all tables
            print(f"\nGenerating models for all {len(tables)} tables...")
            for table in tables:
                generate_and_test_model(generator, table)
            
            print("\nAll models generated successfully!")
            
        finally:
            connector.close()
    else:
        print("Failed to connect to database!")


if __name__ == "__main__":
    run_examples() 