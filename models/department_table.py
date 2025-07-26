"""
Table model for department table.
Generated automatically by sql_data_generator.
"""

import random

from core import TableModel
from core import (StringColumn)

class DepartmentTable(TableModel):
    """Model for department table."""

    def _setup_columns(self):
        """Define the table columns."""
        self.columns = {
            'id': StringColumn(
nullable=False,max_length=4,generator=lambda: "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=4)),) ,
            'dept_name': StringColumn(
nullable=False,max_length=40,) 
        }