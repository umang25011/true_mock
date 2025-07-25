"""
Example table model showing how to use the table model system.
"""

from sql_data_generator.core import (
    TableModel,
    IntegerColumn,
    StringColumn,
    DateTimeColumn,
    EmailColumn,
    NameColumn,
    BooleanColumn
)


class UsersTable(TableModel):
    """Example users table model."""
    
    def _setup_columns(self):
        """Define the table columns."""
        self.columns = {
            'id': IntegerColumn(
                nullable=False,
                unique=True,
                min_value=1,
                max_value=10000
            ),
            'first_name': NameColumn(
                nullable=False,
                name_type='first'
            ),
            'last_name': NameColumn(
                nullable=False,
                name_type='last'
            ),
            'email': EmailColumn(
                nullable=False,
                unique=True
            ),
            'age': IntegerColumn(
                min_value=18,
                max_value=100
            ),
            'is_active': BooleanColumn(
                nullable=False,
                true_probability=0.9  # 90% active users
            ),
            'created_at': DateTimeColumn(
                nullable=False
            )
        }


def main():
    """Example usage of the UsersTable model."""
    # Create table model
    users_table = UsersTable()
    
    # Print table structure
    print(users_table)
    print("\nGenerating sample data:")
    
    # Generate sample data
    sample_data = users_table.generate_rows(5)
    
    # Print sample data
    for row in sample_data:
        print("\nUser:")
        for field, value in row.items():
            print(f"  {field}: {value}")


if __name__ == "__main__":
    main() 