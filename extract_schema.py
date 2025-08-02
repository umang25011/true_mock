"""
Script to extract database schema to JSON.
"""

from core.database import DatabaseConnector, DatabaseOperations
from core.schema import SchemaExtractor

def main():
    # Connect to database
    connector = DatabaseConnector()
    if connector.connect():
        try:
            db_ops = DatabaseOperations(connector)
            extractor = SchemaExtractor(db_ops)
            
            # Extract schema to JSON
            output_file = 'schema.json'
            extractor.extract_schema(output_file)
            print(f"\nSchema extracted to {output_file}")
            
        finally:
            connector.close()

if __name__ == "__main__":
    main() 