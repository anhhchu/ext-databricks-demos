# Databricks Connect Demo Project

This project demonstrates various use cases and capabilities of Databricks Connect, allowing you to develop and run Spark applications locally while leveraging the power of Databricks clusters.

## What is Databricks Connect?

Databricks Connect enables you to connect popular IDEs (PyCharm, VSCode, etc.), notebook servers, and custom applications to Databricks compute. It allows you to:

- Write and debug Spark applications locally
- Leverage Databricks compute power
- Access Unity Catalog and Delta Lake
- Use Databricks ML capabilities from your local environment

## Project Structure

```
databricks-connect-demo/
├── README.md                    # This file
├── TESTING.md                   # Complete testing guide
├── requirements.txt             # Python dependencies
├── run_tests.sh                 # One-click test runner script
├── setup/                      # Setup and configuration scripts
│   ├── setup_python_venv.py   # Python environment & compatibility checker  
│   └── setup_databricks.py    # Databricks profile setup (auth + validation)
├── examples/                   # Sample applications
│   ├── 01_basic_operations/    # Basic Spark operations
│   ├── 02_etl_pipeline/        # ETL workflow examples
│   ├── 03_machine_learning/    # ML model training and inference
│   ├── 04_data_analysis/       # Data analysis and visualization
│   └── 05_streaming/           # Real-time data processing
├── data/                       # Sample datasets
│   ├── sample_data.csv
│   └── weather_data.json
├── config/                     # Configuration files
│   └── cluster_config.json
└── tests/                      # Unit tests
    └── test_examples.py
```

## Prerequisites

1. **Databricks Workspace**: Access to a Databricks workspace with Unity Catalog enabled
2. **Python**: Python 3.10+ (depending on your Databricks Runtime version)
3. **Databricks CLI**: For authentication and cluster management
4. **Cluster Access**: Either a running cluster or access to serverless compute

## Quick Setup

### 1. Set Up Compatible Python Environment

```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

Check Python compatibility and set up virtual environment

```bash
python setup/setup_python_venv.py
```

- **Purpose**: Python environment compatibility checker and virtual environment setup
- **When to use**: Run this first to ensure you have a compatible Python version
- **What it does**:
  - Checks Python version compatibility with Databricks Connect
  - Shows the full compatibility matrix
  - Guides you through virtual environment setup
  - Provides Python installation instructions if needed

### 2. Configure Databricks Profile (One-Step Setup)

```bash
# Interactive profile setup with authentication and validation
python setup/setup_databricks.py
```

- **Profile Configuration**: Choose profile name (e.g., DEFAULT, DEV, PROD)
- **Authentication Setup**: Choose between:
  - Personal Access Token (PAT) - Simple token-based auth
  - OAuth M2M - Service principal with client credentials  
  - OAuth U2M - Interactive browser-based auth
- **Compute Configuration**: Choose between:
  - Serverless compute (recommended) - `serverless_compute_id = auto`
  - Cluster-based compute - Specify your cluster ID
- **Connection Validation**: Comprehensive testing of your setup

The configuration is stored in `~/.databrickscfg`:
```ini
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = dapi123...  # for PAT authentication
serverless_compute_id = auto  # for serverless compute
```

### 3. Run Test Suite (Optional)

```bash
# One-click test runner
./run_tests.sh
```

## Simplified Setup Summary

```bash
# 1. Python environment and dependencies
python setup/setup_python_venv.py
pip install -r requirements.txt

# 2. Configure Databricks profile (one command!)
python setup/setup_databricks.py
```

**Benefits of Profile-based Configuration:**
- ✅ More secure (no credentials in project files)
- ✅ Easier profile switching (DEV/PROD environments)
- ✅ Follows Databricks best practices
- ✅ No environment variable conflicts
- ✅ Works seamlessly with Databricks CLI

📖 **For complete testing instructions, see [TESTING.md](TESTING.md)**

## Example Use Cases

### 1. Basic Data Operations
- Reading from Delta Lake tables
- Simple data transformations
- Writing to Delta Lake

### 2. ETL Pipeline
- Multi-source data ingestion
- Data cleaning and transformation
- Incremental data loading

### 3. Machine Learning
- Feature engineering
- Model training with MLflow
- Model deployment and inference

### 4. Data Analysis
- Exploratory data analysis
- Statistical computations
- Data visualization with Plotly

### 5. Streaming Data
- Real-time data processing
- Structured streaming examples
- Kafka integration

## Running Examples

Each example directory contains its own README with specific instructions. Generally:

```bash
# Navigate to an example directory
cd examples/01_basic_operations

# Run with default profile
python basic_queries.py

# Run with specific profile
python basic_queries.py DEV
python basic_queries.py PROD
```

**Profile Usage**: All examples support specifying a profile name as the first argument. This allows you to easily switch between different environments (DEV, STAGING, PROD) or authentication methods.



### Version Compatibility

| Python Version | Databricks Connect | Databricks Runtime | Installation Command |
|----------------|-------------------|-------------------|---------------------|
| 3.12           | 16.1+ to 17.0+    | 16.1+ to 17.0+    | `pip install "databricks-connect==16.4.*"` |
| 3.11           | 15.1 to 15.4      | 15.1 to 15.4      | `pip install "databricks-connect==15.4.*"` |
| 3.10           | 13.3 to 14.3      | 13.3 to 14.3      | `pip install "databricks-connect==14.3.*"` |

**Important**: Your Databricks Connect version should match your cluster's Databricks Runtime version.

## Best Practices

1. **Virtual Environment**: Always use a virtual environment
2. **Version Matching**: Match Databricks Connect version with your cluster's runtime
3. **Authentication**: Use OAuth for better security
4. **Resource Management**: Clean up temporary tables and files
5. **Testing**: Write unit tests for your Spark applications

## Troubleshooting

### Common Issues

1. **Python Version Compatibility Error**:
   ```
   ERROR: Could not find a version that satisfies the requirement databricks-connect==16.4.*
   ```
   **Solution**: Check your Python version and install the compatible Databricks Connect version:
   ```bash
   # Check Python version
   python --version
   
   # Install compatible version
   # For Python 3.12:
   pip install "databricks-connect==16.4.*"
   
   # For Python 3.11:
   pip install "databricks-connect==15.4.*"
   
   # For Python 3.10:
   pip install "databricks-connect==14.3.*"
   ```

2. **Connection Errors**: Verify authentication and cluster status
3. **Version Mismatch**: Ensure Databricks Connect version matches runtime
4. **PySpark Conflicts**: Uninstall PySpark before installing Databricks Connect
   ```bash
   pip uninstall pyspark
   pip install "databricks-connect==*.*.*"  # or your compatible version
   ```

### Debugging Tips

```python
# Check connection details
print(f"Host: {spark.client.host}")
print(f"User: {spark.client._user_id}")

# Enable debug logging
import os
os.environ['SPARK_CONNECT_LOG_LEVEL'] = 'debug'
```

## Contributing

Feel free to add more examples or improve existing ones. Please:

1. Follow the existing code structure
2. Add proper documentation
3. Include error handling
4. Test your examples thoroughly

## License

This project is for educational and demonstration purposes. 