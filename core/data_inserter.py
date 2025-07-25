"""
Database insertion handler for generated data.
"""

import logging
from typing import List, Dict, Any
import time
from datetime import datetime
from sqlalchemy import text
from .database_connector import DatabaseConnector
from .base_table import TableModel

class DataInserter:
    """Handles insertion of generated data into database."""

    def __init__(self, db_connector: DatabaseConnector):
        """Initialize with database connector."""
        if not db_connector.is_connected():
            if not db_connector.connect():
                raise RuntimeError("Database not connected. Call connect() first.")
        
        self.db_connector = db_connector
        self.engine = db_connector.get_engine()
        self.schema = db_connector.get_schema()
        self.logger = logging.getLogger(__name__)

    def _format_value(self, value: Any) -> Any:
        """Format value for SQL insertion."""
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    def _prepare_data(self, data: List[Dict[str, Any]], table_model: TableModel) -> List[Dict[str, Any]]:
        """Prepare data for batch insertion."""
        # Get columns that should be included in the insert
        included_columns = [
            name for name, col in table_model.columns.items() 
            if not getattr(col, 'skip_generation', False)
        ]
        
        # Filter and format the data
        prepared_data = []
        for row in data:
            prepared_row = {
                k: self._format_value(v) 
                for k, v in row.items() 
                if k in included_columns
            }
            prepared_data.append(prepared_row)
        
        return prepared_data

    def insert_batch(self, table_name: str, data: List[Dict[str, Any]], table_model: TableModel) -> bool:
        """Insert a batch of data."""
        if not data:
            return True

        try:
            # Get columns that should be included in the insert
            included_columns = [
                name for name, col in table_model.columns.items() 
                if not getattr(col, 'skip_generation', False)
            ]
            
            prepared_data = self._prepare_data(data, table_model)
            
            # Construct the insert statement using only included columns
            column_list = ', '.join(included_columns)
            value_list = ', '.join(f':{col}' for col in included_columns)
            insert_query = text(f"""
                INSERT INTO {self.schema}.{table_name} 
                ({column_list}) 
                VALUES ({value_list})
            """)
            
            # Execute batch insert
            with self.engine.begin() as conn:
                conn.execute(insert_query, prepared_data)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error inserting batch into {table_name}: {str(e)}")
            return False

    def insert_table_data(self, table_model: TableModel) -> bool:
        """Insert all data for a table."""
        table_name = table_model.get_table_name()
        total_rows = table_model.rows_per_table
        batch_size = table_model.batch_size
        
        self.logger.info(f"Starting data insertion for table {table_name}")
        start_time = time.time()
        
        try:
            rows_inserted = 0
            while rows_inserted < total_rows:
                remaining = min(batch_size, total_rows - rows_inserted)
                batch_data = table_model.generate_rows(remaining)
                
                if not self.insert_batch(table_name, batch_data, table_model):
                    self.logger.error(f"Failed to insert batch in {table_name}")
                    return False
                
                rows_inserted += len(batch_data)
                self.logger.info(f"Inserted {rows_inserted}/{total_rows} rows in {table_name}")
            
            duration = time.time() - start_time
            self.logger.info(f"Completed inserting {rows_inserted} rows in {table_name}. Duration: {duration:.2f}s")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during data insertion for {table_name}: {str(e)}")
            return False

class BatchInserter:
    """Handles insertion of multiple tables with dependencies."""

    def __init__(self, db_connector: DatabaseConnector):
        """Initialize with database connector."""
        self.inserter = DataInserter(db_connector)
        self.logger = logging.getLogger(__name__)

    def insert_tables(self, tables: List[TableModel]) -> Dict[str, bool]:
        """Insert data for multiple tables."""
        results = {}
        
        try:
            for table in tables:
                table_name = table.get_table_name()
                success = self.inserter.insert_table_data(table)
                results[table_name] = success
                
                if not success:
                    self.logger.warning(f"Failed to insert data for {table_name}")
        
        except Exception as e:
            self.logger.error(f"Error during batch insertion: {str(e)}")
        
        return results

def insert_all_tables(db_connector: DatabaseConnector, tables: List[TableModel]) -> Dict[str, bool]:
    """Convenience function to insert data for all tables."""
    inserter = BatchInserter(db_connector)
    return inserter.insert_tables(tables)

def insert_single_table(db_connector: DatabaseConnector, table: TableModel) -> bool:
    """Convenience function to insert data for a single table."""
    inserter = DataInserter(db_connector)
    return inserter.insert_table_data(table) 