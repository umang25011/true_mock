"""
Table model for employee table.
Generated automatically by sql_data_generator.
"""

from datetime import timedelta, datetime
import random

from core import TableModel
from core import (StringColumn, DateTimeColumn, IntegerColumn, NameColumn)

class EmployeeTable(TableModel):
    """Model for employee table."""

    def _setup_columns(self):
        """Define the table columns."""
        self.columns = {
            'id': IntegerColumn(
nullable=False,min_value=1,max_value=1000000,) ,
            'birth_date': DateTimeColumn(
nullable=False,generator=lambda: datetime.now() - timedelta(days=random.randint(365*20, 365*60)),) ,
            'first_name': NameColumn(
nullable=False,max_length=14,    name_type="first"
) ,
            'last_name': NameColumn(
nullable=False,max_length=16,    name_type="last"
) ,
            'gender': StringColumn(
nullable=False,max_length=1,) ,
            'hire_date': DateTimeColumn(
nullable=False,generator=lambda: datetime.now() - timedelta(days=random.randint(0, 365*10)),) 
        }