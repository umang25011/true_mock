"""
Table model for title table.
Generated automatically by sql_data_generator.
"""

from faker import Faker

fake = Faker()

from core import TableModel
from core import (ReferenceColumn, StringColumn, DateTimeColumn)

class TitleTable(TableModel):
    """Model for title table."""

    def _setup_columns(self):
        """Define the table columns."""
        self.columns = {
            'employee_id': ReferenceColumn(
nullable=False,    to_table="employee",
    to_column="id",
    db_connector=self.db_connector
) ,
            'title': StringColumn(
nullable=False,max_length=50,) ,
            'from_date': DateTimeColumn(
nullable=False,generator=lambda: fake.date_time_this_decade(),) ,
            'to_date': DateTimeColumn(
generator=lambda: fake.date_time_this_decade(),) 
        }