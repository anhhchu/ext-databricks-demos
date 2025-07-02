# Basic Operations with Databricks Connect

This example demonstrates fundamental Databricks Connect operations including:

- Connecting to Databricks using configuration profiles
- Reading data from various sources
- Basic DataFrame operations

## Files

- `basic_queries.py` - Simple data querying and transformations
- `delta_operations.py` - Delta Lake read/write operations
- `sql_examples.py` - SQL query examples
- `data_sources.py` - Reading from multiple data sources

## Prerequisites

1. **Databricks Connect configured**: Run the setup script to configure your profile:
   ```bash
   cd ../../setup
   python setup_databricks.py
   ```

2. **Configuration Profile**: Your `~/.databrickscfg` file should contain a profile like:
   ```ini
   [DEFAULT]
   host = https://your-workspace.cloud.databricks.com
   token = dapi123...  # for PAT authentication
   serverless_compute_id = auto  # for serverless compute
   ```

3. **Required packages**: Install dependencies:
   ```bash
   pip install -r ../../requirements.txt
   ```

## Running the Examples

### Using Default Profile
```bash
# Basic data operations
python basic_queries.py          # Use default profile
```

### Using Specific Profile
```bash
# Use a specific profile (e.g., DEV, PROD, PERSONAL)
python basic_queries.py DEV
```

## Configuration Options

The examples support different authentication and compute configurations:

### Authentication Types
- **Personal Access Token (PAT)**: Simple token-based authentication
- **OAuth M2M**: Service principal with client credentials  
- **OAuth U2M**: Interactive browser-based authentication

### Compute Types
- **Serverless**: `serverless_compute_id = auto` (recommended)
- **Cluster-based**: `cluster_id = your-cluster-id`

## What You'll Learn

1. **Profile Management**: How to configure and use Databricks profiles
2. **Connection Management**: Establishing secure connections to Databricks
3. **DataFrame Operations**: Creating, transforming, and analyzing DataFrames
4. **Delta Lake**: Reading from and writing to Delta Lake tables
5. **SQL Integration**: Running SQL queries on DataFrames and tables
6. **Data Sources**: Working with CSV, JSON, and other data formats

## Troubleshooting

If you encounter connection issues:

1. **Check your profile configuration**:
   ```bash
   cat ~/.databrickscfg
   ```

2. **Verify authentication** (for OAuth U2M):
   ```bash
   databricks auth login
   ```

3. **Test the connection**:
   ```bash
   cd ../../setup
   python setup_databricks.py
   ```

4. **Check workspace permissions**: Ensure your credentials have access to:
   - Databricks workspace
   - Compute resources (serverless or cluster)
   - Required data and catalogs

## Expected Output

Each script will display:
- Profile and connection information
- Data processing results
- Performance metrics
- Sample data previews 