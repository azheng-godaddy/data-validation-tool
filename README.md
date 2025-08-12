# Data Validation Tool

A comprehensive data validation tool that compares legacy and production tables using AWS Athena with seamless Jupyter notebook integration and LLM-generated SQL queries.

## Install (as a library/CLI)

```bash
# From repo root
pip install .
# or build a wheel and share
python -m build  # pip install build
pip install dist/data_validation-0.1.0-py3-none-any.whl
```

CLI entrypoint after install:
```bash
data-validate --help
```

Embed in code:
```python
from data_validator import DataValidator

validator = DataValidator()
report = validator.validate_tables(
    legacy_table="legacy_db.customers",
    prod_table="prod_db.customers"
)
```

## Features

- **Automated Validation Rules**: Row count, primary key, null value, and schema validations
- **LLM-Powered SQL Generation**: Use natural language to describe custom validation requirements
- **Direct AWS Athena Integration**: Execute queries directly on your data lake
- **Jupyter Notebook Ready**: Rich integration with pandas DataFrames, visualizations, and interactive widgets
- **Interactive CLI**: User-friendly command-line interface with multiple output formats
- **Parallel Query Execution**: Efficient processing with concurrent query execution
- **Extensible Architecture**: Easy to add custom validation rules

## Installation

1. **Clone the repository**:
```bash
git clone <repository-url>
cd data-validation
```

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

3. **Set up configuration**:
Create a `.env` file with your credentials:
```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1
ATHENA_DATABASE=your_database_name
ATHENA_WORKGROUP=primary
ATHENA_OUTPUT_LOCATION=s3://your-athena-results-bucket/

# Jupyter Notebook Configuration (optional)
JUPYTER_OUTPUT_FORMAT=styled
JUPYTER_SHOW_PLOTS=true

# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4
```

## Usage

### Command Line Interface

#### Basic Validation
```bash
python cli.py validate -l legacy_db.customers -p prod_db.customers
```

#### Comprehensive Validation
```bash
python cli.py validate \
  -l legacy_db.orders \
  -p prod_db.orders \
  --primary-keys order_id \
  --null-check-columns customer_id,order_date,total_amount \
  --row-count-tolerance 1.0
```

#### Custom LLM Validation
```bash
python cli.py validate \
  -l legacy_db.products \
  -p prod_db.products \
  --custom-validation "Compare average prices and product counts by category"
```

#### Date Range Validation
```bash
python cli.py validate \
  -l legacy_db.orders \
  -p prod_db.orders \
  --date-column order_date \
  --start-date 2024-01-01 \
  --end-date 2024-03-31 \
  --custom-validation "Compare Q1 2024 order patterns and revenue"
```

#### Custom SQL Validation
```bash
python cli.py custom-sql \
  -l legacy_db.sales \
  -p prod_db.sales \
  --legacy-sql "SELECT COUNT(*), SUM(amount) FROM legacy_db.sales" \
  --prod-sql "SELECT COUNT(*), SUM(amount) FROM prod_db.sales"
```

#### Interactive Mode
```bash
python cli.py interactive -l legacy_db.users -p prod_db.users
```

### Jupyter Notebook Integration

#### Quick Setup in Any Notebook
```python
# Import and setup
from data_validator import DataValidator
from notebook_integration import quick_validate_to_df
import pandas as pd

# Quick validation that returns a styled DataFrame
results_df = quick_validate_to_df('legacy_db.customers', 'prod_db.customers')
display(results_df)
```

#### Full Notebook Experience
```python
from notebook_integration import setup_notebook_environment, create_interactive_widget

# Setup notebook environment
exec(setup_notebook_environment())

# Create interactive widget
exec(create_interactive_widget())
```

#### Generate Complete Notebook Template
```python
from notebook_integration import export_notebook_template

# Creates a full validation notebook
export_notebook_template('legacy_db.orders', 'prod_db.orders')
# Opens: data_validation_legacy_db_orders_vs_prod_db_orders.ipynb
```

### Programmatic Usage

```python
from data_validator import DataValidator

# Initialize validator
validator = DataValidator()

# Basic validation
report = validator.validate_tables(
    legacy_table="legacy_db.customers",
    prod_table="prod_db.customers"
)

# Get results as pandas DataFrame for analysis
df = validator.athena_client.execute_query_to_dataframe(
    "SELECT COUNT(*) as row_count FROM legacy_db.customers"
)

# Custom validation with natural language
report = validator.validate_tables(
    legacy_table="legacy_db.orders",
    prod_table="prod_db.orders",
    custom_validation_request="Compare order totals by region and payment method"
)
```

## Jupyter Notebook Features

### 📊 **Rich Visualizations**
- Status distribution pie charts
- Rule-by-rule validation bar charts  
- Execution time tracking
- Summary statistics

### 🎛️ **Interactive Widgets**
- Dynamic table input fields
- Custom validation request text areas
- Tolerance sliders
- One-click validation execution

### 📋 **Styled DataFrames**
- Color-coded status indicators (green/red/yellow)
- Truncated long values for readability
- Sortable and filterable results

### 🤖 **AI Integration**
- LLM-generated validation summaries
- Natural language insights
- Automated recommendations

## Notebook Examples

### Create Validation Cell
```python
from notebook_integration import create_validation_cell

# Generate code for a notebook cell
cell_code = create_validation_cell(
    legacy_table="legacy_db.products",
    prod_table="prod_db.products", 
    custom_request="Compare pricing strategies across product categories"
)

print(cell_code)
```

### Interactive Widget
```python
from notebook_integration import create_interactive_widget

# Add interactive validation widget to notebook
exec(create_interactive_widget())
```

### Visualization Dashboard
```python
from notebook_integration import create_comparison_chart_cell

# Generate visualization code
chart_code = create_comparison_chart_cell()
exec(chart_code)
```

## Validation Rules

### Built-in Rules

1. **Row Count Validation**: Compares total row counts with configurable tolerance
2. **Primary Key Validation**: Checks PK uniqueness and count consistency
3. **Null Value Validation**: Compares null percentages across columns
4. **Schema Validation**: Validates column names, types, and structure

### Custom Rules

Add your own validation rules by extending the `ValidationRule` class:

```python
from validation_rules import ValidationRule, ValidationResult, ValidationStatus

class CustomRule(ValidationRule):
    def generate_sql(self, legacy_table, prod_table):
        return {
            "legacy_sql": f"SELECT custom_metric FROM {legacy_table}",
            "prod_sql": f"SELECT custom_metric FROM {prod_table}"
        }
    
    def validate(self, legacy_result, prod_result):
        # Your validation logic here
        pass

# Add to validator
validator.add_validation_rule(CustomRule())
```

## Output Formats

### Table Format (Default)
Rich formatted tables with color-coded status indicators.

### JSON Format
```bash
python cli.py validate -l table1 -p table2 -o json
```

### CSV Format
```bash
python cli.py validate -l table1 -p table2 -o csv
```

### Pandas DataFrame (Notebook)
```python
# Direct DataFrame output for analysis
results_df = quick_validate_to_df('legacy_db.table', 'prod_db.table')
```

## Configuration

The tool supports the following configuration options:

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | AWS Access Key | Required |
| `AWS_SECRET_ACCESS_KEY` | AWS Secret Key | Required |
| `AWS_REGION` | AWS Region | us-east-1 |
| `ATHENA_DATABASE` | Athena Database | default |
| `ATHENA_WORKGROUP` | Athena Workgroup | primary |
| `ATHENA_OUTPUT_LOCATION` | S3 Results Bucket | Required |
| `JUPYTER_OUTPUT_FORMAT` | Notebook display style | styled |
| `JUPYTER_SHOW_PLOTS` | Enable visualizations | true |
| `OPENAI_API_KEY` | OpenAI API Key | Required |
| `OPENAI_MODEL` | OpenAI Model | gpt-4 |

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI / Jupyter │────│  DataValidator  │────│  AthenaClient   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │                        │
                    ┌─────────────────┐        ┌─────────────────┐
                    │ ValidationRules │        │   SQLGenerator  │
                    └─────────────────┘        └─────────────────┘
                               │                        │
                    ┌─────────────────┐        ┌─────────────────┐
                    │ ValidationResult│        │   OpenAI API    │
                    └─────────────────┘        └─────────────────┘
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Troubleshooting

### Common Issues

1. **Table Access Errors**: Ensure your AWS credentials have access to the specified tables
2. **Athena Query Failures**: Check that your S3 output location is writable
3. **LLM Errors**: Verify your OpenAI API key is valid and has sufficient credits
4. **Schema Validation Issues**: Ensure table names are correctly formatted (database.table)
5. **Notebook Widget Issues**: Install ipywidgets: `pip install ipywidgets && jupyter nbextension enable --py widgetsnbextension`

### Debug Mode

Set environment variable for detailed logging:
```bash
export DEBUG=1
python cli.py validate -l table1 -p table2
```

### Notebook Quick Start

```python
# One-liner to get started in any notebook
!pip install -q data-validation-tool
from notebook_integration import quick_validate_to_df
quick_validate_to_df('your_legacy_table', 'your_prod_table')
``` 