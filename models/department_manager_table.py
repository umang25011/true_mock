"""
Table model for department_manager table.
Generated automatically by sql_data_generator.
"""

from faker import Faker

fake = Faker()

from core import TableModel
from core import (ReferenceColumn, DateTimeColumn)

class DepartmentManagerTable(TableModel):
    """Model for department_manager table."""

    def _setup_columns(self):
        """Define the table columns."""
        self.columns = {
            'employee_id': ReferenceColumn(
nullable=False,    to_table="employee",
    to_column="id",
    db_connector=self.db_connector
) ,
            'department_id': ReferenceColumn(
nullable=False,    to_table="department",
    to_column="id",
    db_connector=self.db_connector
) ,
            'from_date': DateTimeColumn(
nullable=False,generator=lambda: fake.date_time_this_decade(),) ,
            'to_date': DateTimeColumn(
nullable=False,generator=lambda: fake.date_time_this_decade(),) 
        }