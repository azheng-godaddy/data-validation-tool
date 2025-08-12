# Data Validation Tool

A data validation tool for AWS Athena that supports two workflows:
- Single-table profiling (row count, PK uniqueness, schema summary)
- Migration comparison (legacy → prod: row count, PK, schema/type)

No Jupyter required — optimized for use in Cursor and the command line.

## Install (library + CLI)

```bash
# From repo root
pip install .

# Or build a wheel to share
pip install build
python -m build
pip install dist/data_validation-0.1.0-py3-none-any.whl
```

CLI entrypoint after install:
```bash
data-validate --help
```

## Configuration

Create a `.env` (or run the setup command below):
```bash
# AWS
AWS_REGION=us-west-2
ATHENA_OUTPUT_LOCATION=s3://your-athena-results-bucket/

# Optional: GoCode (for LLM-powered SQL)
GOCODE_API_TOKEN=sk-...
GOCAAS_MODEL=claude-3-7-sonnet-20250219
GOCAAS_BASE_URL=https://caas-gocode-prod.caas-prod.prod.onkatana.net

# Optional: GitHub Schema repo enhancement
GITHUB_TOKEN=your_token
GITHUB_REPO_OWNER=gdcorp-dna
GITHUB_REPO_NAME=lake
GITHUB_BRANCH=main
ENABLE_GITHUB_SCHEMA=false
```

Quick setup helper:
```bash
data-validate setup-env
```

## Quick Start (CLI)

- Single-table profiling (no prod table):
```bash
data-validate validate \
  -l ecomm_mart.fact_bill_line \
  -k bill_id,bill_line_num

# With date filter
data-validate validate \
  -l ecomm_mart.fact_bill_line \
  -k bill_id,bill_line_num \
  -d bill_modified_mst_date -s 2025-07-19 -e 2025-07-24
```

- Migration comparison (legacy → prod):
```bash
data-validate validate \
  -l ecomm_mart.fact_bill_line \
  -p enterprise_linked.fact_bill_line \
  -k bill_id,bill_line_num
```

- LLM-powered validation (optional GoCode):
```bash
# Row count comparison between two tables
data-validate llm-validate "compare row counts between tables" \
  -t "ecomm_mart.fact_bill_line,enterprise_linked.fact_bill_line"

# PK integrity using a composite key
data-validate llm-validate "validate primary key uniqueness using bill_id and bill_line_num columns" \
  -t "ecomm_mart.fact_bill_line,enterprise_linked.fact_bill_line" -k "bill_id,bill_line_num"

# Single-table profiling (no prod table)
data-validate llm-validate "profile this table" -t "ecomm_mart.fact_bill_line"

# SCD2: null checks on current records only
data-validate llm-validate "null checks on current records only (where is_current = 'Y')" \
  -t "ecomm_core_conformed_local.dim_subscription_product"

# Mismatch detection with LEFT JOIN and date filtering
data-validate llm-validate \
  "bill mismatch detection: enterprise.dim_new_acquisition_shopper vs enterprise_linked.dim_new_acquisition_shopper, \
   date filter: 2025-07-19 to 2025-07-24, \
   join condition: bill_shopper_id matches but new_acquisition_bill_id differs, \
   output: COUNT of total mismatches, query type: single LEFT JOIN query"

# Bi-directional missing records (UNION of LEFT JOINs)
data-validate llm-validate \
  "find missing records in both directions between enterprise.dim_new_acquisition_shopper and enterprise_linked.dim_new_acquisition_shopper, \
   date filter: 2025-07-19 to 2025-07-24, join condition: bill_shopper_id matches, \
   output: show all missing records with source indicator, query type: UNION of LEFT JOINs"
```
Tips:
- You can omit `-t`/`-k` and let the tool auto-extract tables, keys, and dates from your prompt.
- GoCode requires VPN and a valid `GOCODE_API_TOKEN` in `.env`.

- Output formats:
```bash
# table (default)
data-validate validate -l table1 -p table2 -o table
# json
data-validate validate -l table1 -p table2 -o json
# csv
data-validate validate -l table1 -p table2 -o csv
```

## Programmatic Usage (Python)

```python
from data_validator import DataValidator

validator = DataValidator()
report = validator.validate_tables(
    legacy_table="legacy_db.customers",
    prod_table="prod_db.customers"
)
print(report.summary)
```

## Troubleshooting

- SSO/AWS credentials: verify your environment and `.env`
- GoCode errors: check VPN and `GOCODE_API_TOKEN`
- Table access: confirm table names exist in Glue and you have permissions

## Versioning & Releases

- Bump `version` in `pyproject.toml`
- Tag releases: `git tag v0.1.0 && git push origin v0.1.0`

## License

MIT License - see LICENSE. 