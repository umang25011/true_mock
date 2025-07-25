"""
Table model for employee table.
Generated automatically by sql_data_generator.
"""

from datetime import datetime, timedelta
import random
from faker import Faker

fake = Faker()

from core import TableModel
from core import (DateTimeColumn, IntegerColumn, NameColumn, StringColumn)


class EmployeeTable(TableModel):
    """Model for employee table."""

    def _setup_columns(self):
        """Define the table columns."""
        self.columns = {
            'id': IntegerColumn(
                nullable=False,
                skip_generation=True
            ),
            'birth_date': DateTimeColumn(
                nullable=False,
                generator=lambda: datetime.now() - timedelta(days=random.randint(365*20, 365*60))
            ),
            'first_name': NameColumn(
                nullable=False,
                name_type="first",
                max_length=14,
                generator=lambda: fake.first_name()
            ),
            'last_name': NameColumn(
                nullable=False,
                name_type="last",
                max_length=16,
                generator=lambda: fake.last_name()
            ),
            'gender': StringColumn(
                nullable=False,
                max_length=1,
                generator=lambda: random.choice(["M", "F"])
            ),
            'hire_date': DateTimeColumn(
                nullable=False,
                generator=lambda: datetime.now() - timedelta(days=random.randint(0, 365*10))
            )
        }
