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
    """Load specific table models."""
    models = []
    models_dir = 'models'
    
    # Only load employee and salary tables
    target_tables = ['employee_table.py', 'salary_table.py']
    
    for filename in target_tables:
        if os.path.exists(os.path.join(models_dir, filename)):
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
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Set up database connection
    db_connector = DatabaseConnector()
    if not db_connector.connect():
        logger.error("Failed to connect to database")
        return

    try:
        # Load employee and salary models
        models = load_table_models()
        
        # First insert employee data
        employee_model = next((m for m in models if m.get_table_name() == 'employee'), None)
        if employee_model:
            logger.info("Inserting employee data...")
            success = insert_single_table(db_connector, employee_model)
            if not success:
                logger.error("Failed to insert employee data")
                return
            logger.info("Successfully inserted employee data")
        else:
            logger.error("Employee model not found")
            return
            
        # Then insert salary data
        salary_model = next((m for m in models if m.get_table_name() == 'salary'), None)
        if salary_model:
            logger.info("Inserting salary data...")
            success = insert_single_table(db_connector, salary_model)
            if not success:
                logger.error("Failed to insert salary data")
            else:
                logger.info("Successfully inserted salary data")
        else:
            logger.error("Salary model not found")

    except Exception as e:
        logger.error(f"Error during data insertion: {str(e)}")
    
    finally:
        db_connector.close()

if __name__ == '__main__':
    main() 