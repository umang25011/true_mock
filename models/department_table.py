"""
Table model for department table.
Generated automatically by sql_data_generator.
"""

import random
from faker import Faker

fake = Faker()

from core import TableModel
from core import (StringColumn, IntegerColumn)

class DepartmentTable(TableModel):
    """Model for department table."""

    def _setup_columns(self):
        """Define the table columns."""
        self.columns = {
            'id': IntegerColumn(
                nullable=False,
                min_value=1,
                max_value=1000000,
                generator=lambda: random.randint(1, 1000000),
            ),
            'dept_name': StringColumn(
                nullable=False,
                max_length=40,
                generator=lambda: fake.text(max_nb_chars=40),
            ),
        }

 