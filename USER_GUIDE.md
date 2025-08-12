# 📊 Data Validation Tool - User Guide

A powerful data validation solution designed to support single-table data profiling and migration tables validation (legacy → prod), with two validation modes:

Predefined Rules – Standardized checks for data consistency and integrity.

LLM-Powered Analysis – Advanced insights using GoCode (GoDaddy’s AI service).

## 🎯 Available Commands

- **`validate`** - Predefined Validation Rules: The validation framework leverages parameterized SQL templates to streamline query generation across different validation rules
- **`llm-validate`** - LLM-powered profiling and migration validation
- **`setup-env`** - Configure environment settings

## 🎯 Two Use Cases

### Use Case 1: Single-Table Data Profiling
- ✅ Schema & data type summary
- ✅ Data distribution and top categories
- ✅ SCD2 current-record profiling
- ✅ Optional date filtering
- 🧠 Best with `llm-validate` (natural language)

### Use Case 2: Migration Tables Validation (Legacy → Prod)
- ✅ Row count comparison
- ✅ Primary key validation (including composite keys)
- ✅ Schema & data type comparison
- ✅ Date range filtering
- ✅ Mismatch detection and missing-record analysis
- ⚙️ Use `validate` (parameters) or `llm-validate` (natural language)

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- **Cursor IDE** (recommended)
- Access to AWS Athena Notebooks and Glue Data Catalog
- Enterprise SSO credentials (GoDaddy SSO)
- **Optional**: GoCode API token for LLM features
- **Optional**: GitHub token for enhanced schema detection

---

### 1. Setup Environment

1. **Clone and Setup Environment**
```bash
# Navigate to the project directory
cd data-validation

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

2. **SSO Authentication**
```bash
# Install SSO tools
pip install aws-okta-processor

# Authenticate with SSO
eval $(aws-okta-processor authenticate -e -o godaddy.okta.com -u username)

# Verify authentication
python3 setup_aws.py check-credentials
```

3. **Configure Environment**
```bash
# Automated setup with Athena Notebooks
python3 cli.py setup-env
```

This will prompt for:
- **AWS Athena Notebooks configuration** (automatic for SSO users)
- **GoCode API token** (optional - enhances profiling and LLM validation)
- **GitHub token** (optional - enhances LLM SQL with real schemas)

**How to get GoCode API token:**
1. Visit: [https://caas.godaddy.com/gocode/my-api-keys]
2. Create new API key
3. Copy the token (starts with `sk-`)
4. **Requires GoDaddy VPN connection**

---

## 🎯 Usage Examples

### Use Case 1: Single-Table Data Profiling
LLM-powered profiling with no manual SQL required.
```bash
# Basic profiling
python3 cli.py llm-validate "profile this table" -t "enterprise_linked.dim_subscription"

# Analyze distributions and outliers
python3 cli.py llm-validate "analyze data distribution and top categories for all columns" -t "ecomm_mart.fact_bill_line"

# Null checks on current SCD2 records only
python3 cli.py llm-validate "null checks on current records only (where is_current = 'Y')" -t "ecomm_core_conformed_local.dim_subscription_product"

# Schema summary + basic stats
python3 cli.py llm-validate "show schema, data types, and basic stats for each column" -t "ecomm_mart.fact_bill_line"
```

Predefined (no LLM, single-table using `validate`):
```bash
# Single-table validation with composite primary key (row count, PK uniqueness, schema summary)
python3 cli.py validate \
  -l "ecomm_mart.fact_bill_line" \
  -k "bill_id,bill_line_num"

# With date filtering
python3 cli.py validate \
  -l "ecomm_mart.fact_bill_line" \
  -k "bill_id,bill_line_num" \
  -d "bill_modified_mst_date" \
  -s "2025-07-19" \
  -e "2025-07-24"
```

### Use Case 2: Migration Tables Validation (Legacy → Prod)
Compare two tables for counts, keys, schema differences, and mismatches.

Predefined (parameter-based, automation-friendly):
```bash
# Row count and key checks (composite PK example)
python3 cli.py validate \
  -l "ecomm_mart.fact_bill_line" \
  -p "enterprise_linked.fact_bill_line" \
  -k "bill_id,bill_line_num"

# Full validation with date range filtering
python3 cli.py validate \
  -l "enterprise_linked.dim_bill_shopper_id_xref" \
  -p "enterprise.dim_bill_shopper_id_xref_lookalike" \
  -k "bill_id" \
  -d "bill_modified_mst_date" \
  -s "2025-07-19" \
  -e "2025-07-24"
```

LLM-powered (natural language, flexible):
```bash
# Row count comparison
python3 cli.py llm-validate "compare row counts between tables" -t "ecomm_mart.fact_bill_line,enterprise_linked.fact_bill_line"

# PK integrity using a composite key
python3 cli.py llm-validate "validate primary key uniqueness using bill_id and bill_line_num columns" \
  -t "ecomm_mart.fact_bill_line,enterprise_linked.fact_bill_line" -k "bill_id,bill_line_num"

# Mismatch detection with join logic (single LEFT JOIN)
python3 cli.py llm-validate \
  "bill mismatch detection: enterprise.dim_new_acquisition_shopper vs enterprise_linked.dim_new_acquisition_shopper, \
   date filter: 2025-07-19 to 2025-07-24, \
   join condition: bill_shopper_id matches but new_acquisition_bill_id differs, \
   output: COUNT of total mismatches, query type: single LEFT JOIN query"

# Bi-directional missing records (UNION of LEFT JOINs)
python3 cli.py llm-validate \
  "find missing records in both directions between enterprise.dim_new_acquisition_shopper and enterprise_linked.dim_new_acquisition_shopper, \
   date filter: 2025-07-19 to 2025-07-24, join condition: bill_shopper_id matches, \
   output: show all missing records with source indicator, query type: UNION of LEFT JOINs"
```

---

## 📊 Output Formats

```bash
# Table format (default)
python3 cli.py validate --output-format table

# JSON output
python3 cli.py llm-validate "query" -t "tables" --output-format json

# CSV output
python3 cli.py validate --output-format csv
```

---

### Status Types
- **PASS**: Validation succeeded
- **FAIL**: Validation failed - requires attention
- **INFO**: Informational data comparison
- **ERROR**: Technical error occurred

---

### Common Issues

#### 1. **"Missing required configuration" Error**
```bash
# Solution: Run setup again  
python3 cli.py setup-env
```

#### 2. **SSO Authentication Issues**
```bash
# Check SSO status
echo "$([ -n "$AWS_SESSION_TOKEN" ] && echo 'SSO: ACTIVE ✅' || echo 'SSO: EXPIRED ❌')"

# Re-authenticate if expired
eval $(aws-okta-processor authenticate -e -o godaddy.okta.com -u username)
```

#### 3. **GoCode API Issues** (LLM Mode)
If you see GoCode connection errors:

```bash
# Check if GoCode is configured
python3 -c "from config import settings; print(f'GoCode Token: {settings.gocode_api_token[:20] if settings.gocode_api_token else \"Not configured\"}...')"

# Common solutions:
# 1. Verify VPN connection to GoDaddy network
# 2. Re-run setup with correct token
python3 cli.py setup-env

# 3. Test basic validation without LLM (requires parameters)
python3 cli.py validate -l your_legacy_table -p your_prod_table
```

**Common GoCode errors:**
- `No AI service credentials found` - GoCode not configured, use predefined mode
- `Failed to connect to GoCode API` - Check VPN connection and token
- `GoCode API error: 400` - Invalid request format (usually auto-fixed)
- `GoCode API error: 401` - Invalid token, get new one from GoCode portal

#### 4. **Table Access Issues**
```bash
# Verify table names
python3 setup_aws.py list-tables --database your_database

# Check table access
python3 -c "
from athena_client import AthenaClient
client = AthenaClient()
result = client.test_table_access('ecomm_mart.fact_bill_line')
print(f'Table access: {"✅ SUCCESS" if result else "❌ FAILED"}')
"
```

---

## 📋 Use Case Comparison

| Feature | Use Case 1: Profiling | Use Case 2: Migration Validation |
|---------|------------------------|----------------------------------|
| **Setup Required** | AWS SSO + optional GoCode | AWS SSO (predefined) + optional GoCode |
| **Row Count Comparison** | ❌ | ✅ |
| **Primary Key Validation** | ❌ | ✅ |
| **Schema Summary** | ✅ | ✅ |
| **Data Distribution** | ✅ | ❌ |
| **Null Checks** | ✅ | ✅ (targeted) |
| **Date Range Filtering** | ✅ | ✅ |
| **Natural Language Input** | ✅ | ✅ |
| **Parameterized CLI** | ❌ | ✅ |
| **Business Logic Joins** | ❌ | ✅ |

---

## 🎯 Best Practices

### 1. Choose the Right Use Case
- Use **Profiling** (`llm-validate`) to explore a single table quickly
- Use **Migration Validation** (`validate` or `llm-validate`) to compare legacy vs prod tables

### 2. Parameters vs Natural Language
- **Parameters (`validate`)**: deterministic, automation-friendly, great for CI
- **Natural language (`llm-validate`)**: flexible exploration, complex joins and business rules

### 3. SSO Management
- Re-authenticate every 2-4 hours (credentials expire)
- Check SSO status before long validation runs: `echo $AWS_SESSION_TOKEN`
- Keep terminal session active

### 4. Query Optimization
- Use date range filtering for large tables with `-d`, `-s`, `-e` parameters
- Test with small datasets first
- Specify composite keys like `bill_id,bill_line_num` for line-item tables

### 5. Error Handling
- Always verify SSO authentication first
- Check table names in AWS Glue console
- Use predefined mode if LLM mode fails

---

## 🗄️ SQL Cache System

The tool automatically caches LLM-generated SQL queries to improve performance and reduce API costs.

### Features
- ⚡ Fast Retrieval: 20-50x faster for repeated queries (< 0.1s vs 2-5s)
- 💾 Persistent Storage: Cache survives application restarts
- 🔄 Smart Expiration: 24-hour TTL (configurable)
- 📊 Performance Tracking: Hit/miss rates and usage statistics

### Configuration (Optional)
Add to your `.env` file to customize:
```bash
ENABLE_SQL_CACHE=true          # Enable/disable caching
SQL_CACHE_TTL_HOURS=24         # Cache expiration (hours)
SQL_CACHE_MAX_ENTRIES=1000     # Maximum cache entries
```

### Cache Management Commands
```bash
# View cache statistics and performance
python cli.py cache-stats

# Clear all cached queries
python cli.py cache-clear

# Show current configuration
python cli.py cache-config
```

### How It Works
1. First Request: `llm-validate "count rows"` → LLM generates SQL → Cache stores result
2. Repeat Request: `llm-validate "count rows"` → Cache hit → Instant result ⚡

The cache automatically handles:
- Different table names, validation requests, and date filters
- Schema changes and parameter variations
- Automatic cleanup of expired entries

---

## 🎉 You're Ready to Go!

The tool is now configured and ready for production use. All commands automatically use Athena Notebooks for optimal performance with both traditional and modern table formats, plus intelligent SQL caching for lightning-fast repeat queries. 