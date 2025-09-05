# Data Validation Tool

A data validation tool for AWS Athena that supports two workflows:
- Single-table profiling (row count, PK uniqueness, schema summary)
- Migration comparison (legacy → prod: row count, PK, schema/type)

**Documentation**: [AI-Powered Data Validation Framework](https://godaddy-corp.atlassian.net/wiki/spaces/~azheng/pages/3869468704/AI-Powered+Data+Validation+Framework)

## Get the code locally

- Download ZIP: [Download repository ZIP](https://github.com/azheng-godaddy/data-validation-tool/archive/refs/heads/main.zip) and unzip it
- Or clone the repo:
```bash
git clone https://github.com/azheng-godaddy/data-validation-tool.git
```
Then:
```bash
cd data-validation-tool
```

## Install (library + CLI)

```bash
# Recommended: create and activate a virtual environment (macOS/Linux)
python3 -m venv .venv
source .venv/bin/activate

# From repo root, install the tool into the venv
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

## Run setup

Create `.env` interactively (recommended):
```bash
data-validate setup-env
```
- Prompts for GoCode token and GitHub token
- Prompts for Athena S3 output location. Press Enter to use default:
  - **ATHENA_OUTPUT_LOCATION=s3://aws-athena-query-results-255575434142-us-west-2/**

Non-interactive (CI-friendly):
```bash
data-validate setup-env \
  -a s3://my-athena-results-bucket/prefix/ \
  -t sk-... \
  -g ghp_...
```

Note: You can override `ATHENA_OUTPUT_LOCATION` per run using `-a` on `validate` or `llm-validate` if needed.

## SSO authentication (required)

```bash
pip install aws-okta-processor
# Authenticate (replace 'username' with your SSO username)
eval $(aws-okta-processor authenticate -e -o godaddy.okta.com -u username)
# Verify credentials
python3 setup_aws.py check-credentials
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

# Optional: override S3 output for this run
data-validate validate -l ecomm_mart.fact_bill_line -k bill_id,bill_line_num \
  -a s3://my-athena-results/your-prefix/

# Column comparison using lake schema (per-column mismatch counts)
data-validate compare-columns -l ecomm360.fact_bill_line_vw -p enterprise_linked.fact_bill_line -k bill_id,bill_line_num -d bill_modified_mst_date -s 2025-07-19 -e 2025-07-24
```

- Migration comparison (legacy → prod):
```bash
data-validate validate \
  -l ecomm_mart.fact_bill_line \
  -p enterprise_linked.fact_bill_line \
  -k bill_id,bill_line_num
```

- LLM-powered validation (GoCode):
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


### Column-to-column comparison (sample rows for a specific column)
data-validate llm-validate "mismatch check between legacy and prime.
legacy: enterprise_linked.fact_bill_line l inner join enterprise_linked.fact_bill b on l.bill_id = b.bill_id.
prime: ecomm360.fact_bill_line_vw.
primary key: bill_id,bill_line_num.
columns to compare: tax_usd_amt,chargeback_flag,department_id.
output: TOP 20 mismatched rows with bill_id,bill_line_num and both values for each column.
join type: INNER JOIN on primary key.
date filter: bill_modified_mst_date between 2025-07-19 and 2025-07-24"


data-validate llm-validate "column-by-column comparison between ecomm360.fact_bill_line_vw and enterprise_linked.fact_bill_line, primary key: bill_id,bill_line_num, columns: subaccount_customer_id,item_tracking_code,pf_id, output: COUNT mismatches per column, join type: INNER JOIN on primary key" \
  -d bill_modified_mst_date -s 2025-07-19 -e 2025-07-24



# Optional: per-run S3 override for LLM mode
data-validate llm-validate "compare row counts" -t "db1.t1,db2.t2" \
  -a s3://org-athena-results-123456789012/region/
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