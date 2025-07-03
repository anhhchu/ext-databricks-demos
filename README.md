# 🏗️ Databricks Demo Examples

This repository contains two main demonstration areas for Databricks technologies:

---

## 📊 **databricks-connect/** 
**Complete Databricks Connect Development Environment**

This is the main directory containing a comprehensive demonstration of Databricks Connect capabilities. It's structured as a complete development project with:

### 🔧 **Setup & Configuration**
- `setup/` - Authentication, environment setup, and connection validation scripts
- `requirements.txt` - All Python dependencies

### 📚 **Examples** (Progressive Learning Path)
1. **`01_basic_operations/`** - Fundamental operations
   - Basic Spark DataFrame operations
   - Delta Lake read/write operations  
   - SQL query examples
   - Multi-source data reading

2. **`02_etl_pipeline/`** - Production ETL workflows
   - Complete Extract-Transform-Load pipeline
   - Data quality checks and validation
   - Incremental data loading patterns
   - Multi-source data processing (CSV, JSON, Delta Lake)

3. **`03_machine_learning/`** - ML lifecycle management
   - Feature engineering with Spark
   - Multiple ML model training (Linear Regression, Random Forest, etc.)
   - MLflow experiment tracking and model registry
   - Batch inference and model deployment

### 📁 **Sample Data**
- `data/` - Sample CSV and JSON files for testing examples

---

## 🏔️ **iceberg/snowflake/** 
**Snowflake-Databricks Iceberg Integration**

This directory contains SQL scripts for setting up Iceberg table format integration between Snowflake and Databricks:

### 🔐 **Authentication & Setup**
- `sf-create-catalog-integration.sql` - Creates Snowflake catalog integration with Databricks Unity Catalog using OAuth and PAT authentication
- `sf-create-external-volumes.sql` - Sets up external storage volumes

### 📊 **Demo Data & Tables**
- `sf-create-tpch-external-iceberg.sql` - Creates TPC-H benchmark tables in external Iceberg format
- `sf-create-tpch-managed-iceberg.sql` - Creates TPC-H tables as managed Iceberg tables
- `sf-create-tpch-iceberg-delta.sql` - Demonstrates Iceberg to Delta Lake integration

### 🚀 **Benchmark Queries**
- `sf-tpch-bm-queries.sql` - TPC-H benchmark queries (754 lines)
- `sf-tpcds-bm-queries.sql` - TPC-DS benchmark queries

---

## 🎯 **How to Navigate**

**For Databricks Connect Learning:**
1. Start with `databricks-connect/README.md` for setup
2. Follow examples in order: `01_basic_operations` → `02_etl_pipeline` → `03_machine_learning`
3. Use `run_tests.sh` to validate your setup

**For Iceberg Integration:**
1. Use `iceberg/snowflake/sf-create-catalog-integration.sql` first to set up connectivity
2. Then create tables with the other SQL scripts
3. Run benchmark queries to test performance

Each directory contains detailed README files with specific instructions, prerequisites, and expected outputs.
