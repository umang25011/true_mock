# True Mock - SQL to Python Model Generator

A powerful Python tool that automatically generates Python model classes from your SQL database schema and provides built-in functionality to generate realistic random test data.

## Overview

True Mock simplifies the development and testing process by:
- Automatically creating Python model classes from your SQL database schema
- Generating random but realistic test data that matches your database constraints
- Supporting various column types with customizable data generation rules

## Features

- 🔄 **Automatic Model Generation**
  - Converts SQL tables to Python classes with predefined data generators
  - Supports common column types (Integer, String, DateTime, Name, etc.)
  - Customizable data generation rules per column
  
- 🎲 **Smart Random Data Generation**
  - Built-in generators for common data types
  - Uses Faker library for realistic data
  - Respects column constraints (nullable, length, value ranges)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd true_mock
```

2. Set up a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Unix/macOS
# or
.\venv\Scripts\activate  # On Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

Here's a simple example of a generated model:

```python
from core import TableModel
from core import IntegerColumn, StringColumn

class ProductTable(TableModel):
    """Model for product table."""
    
    def _setup_columns(self):
        """Define the table columns."""
        self.columns = {
            'id': IntegerColumn(
                nullable=False,
                min_value=1,
                max_value=1000,
                generator=lambda: random.randint(1, 1000)
            ),
            'name': StringColumn(
                nullable=False,
                max_length=50,
                generator=lambda: fake.product_name()
            ),
            'category': StringColumn(
                nullable=True,
                max_length=20,
                generator=lambda: random.choice(['Electronics', 'Books', 'Clothing'])
            )
        }
```

Generate random data:
```python
# Create product table instance
product_table = ProductTable()

# Generate 5 random products
random_products = product_table.generate_rows(5)

# Example output:
# [
#     {'id': 42, 'name': 'Ergonomic Keyboard', 'category': 'Electronics'},
#     {'id': 157, 'name': 'Cotton T-Shirt', 'category': 'Clothing'},
#     ...
# ]
```

## Column Types

The tool supports various column types:
- `IntegerColumn`: For numeric values with range constraints
- `StringColumn`: For text with length limits
- `DateTimeColumn`: For dates and timestamps
- `NameColumn`: For generating realistic names
- And more...

Each column type can have:
- Custom data generators
- Nullable constraints
- Value range limits
- Length restrictions

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

[Add your chosen license]

## Disclaimer

This tool is for development and testing purposes. Generated data should not be used in production environments. 