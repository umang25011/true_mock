#!/usr/bin/env python3
"""
Data insertion script for generated models.
"""

import argparse
import os
from typing import List
import importlib
import logging
from core.database_connector import DatabaseConnector
from core.data_inserter import insert_all_tables, insert_single_table
from core import TableModel

def load_table_models() -> List[TableModel]:
    """Load all available table models."""
    models = []
    models_dir = 'models'
    
    # Import all table models
    for filename in os.listdir(models_dir):
        if filename.endswith('_table.py'):
            module_name = filename[:-3]  # Remove .py
            module = importlib.import_module(f'models.{module_name}')
            
            # Find the table model class in the module
            for attr_name in dir(module):
                if attr_name.endswith('Table'):
                    table_class = getattr(module, attr_name)
                    if isinstance(table_class, type) and issubclass(table_class, TableModel):
                        models.append(table_class())
    
    return models

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Insert generated data into database tables.')
    parser.add_argument('--table', help='Specific table to populate (omit for all tables)')
    parser.add_argument('--rows', type=int, help='Override number of rows to generate')
    parser.add_argument('--batch-size', type=int, help='Override batch size for insertion')
    args = parser.parse_args()

    # Set up database connection
    db_connector = DatabaseConnector()
    if not db_connector.connect():
        print("Failed to connect to database")
        return

    try:
        if args.table:
            # Find and insert single table
            models = load_table_models()
            table_model = next((m for m in models if m.get_table_name() == args.table.lower()), None)
            
            if not table_model:
                print(f"Table model not found: {args.table}")
                return
            
            # Override settings if provided
            if args.rows:
                table_model.rows_per_table = args.rows
            if args.batch_size:
                table_model.batch_size = args.batch_size
            
            success = insert_single_table(db_connector, table_model)
            print(f"Table {args.table} insertion {'successful' if success else 'failed'}")
        
        else:
            # Insert all tables
            models = load_table_models()
            
            # Override settings if provided
            if args.rows or args.batch_size:
                for model in models:
                    if args.rows:
                        model.rows_per_table = args.rows
                    if args.batch_size:
                        model.batch_size = args.batch_size
            
            results = insert_all_tables(db_connector, models)
            
            # Print results
            print("\nInsertion Results:")
            for table, success in results.items():
                print(f"{table}: {'Success' if success else 'Failed'}")

    except Exception as e:
        print(f"Error during data insertion: {str(e)}")
    
    finally:
        db_connector.close()

if __name__ == '__main__':
    main() 