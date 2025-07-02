# Basic Operations with Databricks Connect

This example demonstrates fundamental Databricks Connect operations including:

- Connecting to Databricks
- Reading data from various sources
- Basic DataFrame operations
- Writing data to Delta Lake
- SQL operations

## Files

- `basic_queries.py` - Simple data querying and transformations
- `delta_operations.py` - Delta Lake read/write operations
- `sql_examples.py` - SQL query examples
- `data_sources.py` - Reading from multiple data sources

## Prerequisites

1. Databricks Connect configured and tested
2. Sample data files in the `data/` directory
3. Required Python packages installed

## Running the Examples

```bash
# Basic data operations
python basic_queries.py

# Delta Lake operations
python delta_operations.py

# SQL examples
python sql_examples.py

# Data sources example
python data_sources.py
```

## What You'll Learn

1. **Connection Management**: How to establish and manage Databricks connections
2. **DataFrame Operations**: Creating, transforming, and analyzing DataFrames
3. **Delta Lake**: Reading from and writing to Delta Lake tables
4. **SQL Integration**: Running SQL queries on DataFrames and tables
5. **Data Sources**: Working with CSV, JSON, and other data formats

## Expected Output

Each script will display:
- Connection information
- Data processing results
- Performance metrics
- Sample data previews 