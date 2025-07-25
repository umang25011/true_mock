"""
Table model for department_employee table.
Generated automatically by sql_data_generator.
"""

import random
from faker import Faker

fake = Faker()

from core import TableModel
from core import (DateTimeColumn, IntegerColumn)
from core.relations import (Relation, RelationConfig, ManyToOneRelation)

class DepartmentEmployeeTable(TableModel):
    """Model for department_employee table."""

    def _setup_columns(self):
        """Define the table columns."""
        self.columns = {
            'employee_id': IntegerColumn(
                nullable=False,
                min_value=1,
                max_value=1000000,
                generator=lambda: random.randint(1, 1000000),
            ),
            'department_id': IntegerColumn(
                nullable=False,
                min_value=1,
                max_value=1000000,
                generator=lambda: random.randint(1, 1000000),
            ),
            'from_date': DateTimeColumn(
                nullable=False,
                generator=lambda: fake.date_time_this_decade(),
            ),
            'to_date': DateTimeColumn(
                nullable=False,
                generator=lambda: fake.date_time_this_decade(),
            ),
        }

    def _setup_relations(self):
        """Define the table relationships."""
        self.relations.append(
            ManyToOneRelation(
                from_table="department_employee",
                to_table="employee",
                from_column="employee_id",
                to_column="id",
                config=RelationConfig(
                    min_related=1,
                    max_related=5,
                    pool_size=10
                )
            )
        )
        self.relations.append(
            ManyToOneRelation(
                from_table="department_employee",
                to_table="department",
                from_column="department_id",
                to_column="id",
                config=RelationConfig(
                    min_related=1,
                    max_related=5,
                    pool_size=10
                )
            )
        )
 