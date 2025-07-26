"""
Table model for salary table.
Generated automatically by sql_data_generator.
"""

from faker import Faker

fake = Faker()

from core import TableModel
from core import (ReferenceColumn, DateTimeColumn, IntegerColumn)

class SalaryTable(TableModel):
    """Model for salary table."""

    def _setup_columns(self):
        """Define the table columns."""
        self.columns = {
            'employee_id': ReferenceColumn(
nullable=False,    to_table="employee",
    to_column="id",
    db_connector=self.db_connector
) ,
            'amount': IntegerColumn(
nullable=False,min_value=1,max_value=1000000,) ,
            'from_date': DateTimeColumn(
nullable=False,generator=lambda: fake.date_time_this_decade(),) ,
            'to_date': DateTimeColumn(
nullable=False,generator=lambda: fake.date_time_this_decade(),) 
        }