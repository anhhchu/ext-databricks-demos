# Basic Operations Examples

This directory contains examples demonstrating fundamental Databricks Connect operations including DataFrame manipulations, queries, and Delta Lake operations.

## Files

### `basic_queries.py`
Comprehensive example showing:
- DataFrame creation and basic operations
- Filtering and aggregation
- Advanced transformations with window functions
- Working with sample data

### `delta_operations.py` 
Dedicated script for Delta Lake operations:
- Reading data from `sample_data.csv` and `weather_data.json`
- Writing to Delta tables in `main.default` catalog/schema
- Table verification and analytics queries
- Delta table details and history

## Data Files Used

The examples work with data files located in `../../data/`:
- `sample_data.csv`: Employee data with columns (id, name, department, salary, hire_date, location, age)
- `weather_data.json`: Weather data with fields (date, location, temperature, humidity, weather_condition)

## Delta Tables Created

Both scripts create the following Delta tables:
- `main.default.employees` - Employee data from CSV
- `main.default.weather` - Weather data from JSON

## Usage

### Running Basic Queries (includes Delta operations)
```bash
# Use default profile
python basic_queries.py

# Use specific profile
python basic_queries.py DEV
```

### Running Delta Operations Only
```bash
# Use default profile
python delta_operations.py

# Use specific profile  
python delta_operations.py PROD
```

## What the Delta Operations Do

1. **Read CSV Data**: Loads employee data from `sample_data.csv` with proper schema inference
2. **Read JSON Data**: Loads weather data from `weather_data.json`
3. **Write to Delta**: Creates Delta tables in the main catalog:
   - Uses `overwrite` mode to replace existing data
   - Enables schema evolution with `overwriteSchema` option
4. **Verify Tables**: Confirms tables were created and checks row counts
5. **Analytics Queries**: Demonstrates SQL queries on the Delta tables:
   - Employee analytics by department
   - Weather analytics by location
   - Cross-table analysis joining employees and weather data
6. **Table Details**: Shows Delta table properties and history

## Prerequisites

1. **Databricks Connect Setup**: Run the setup script first:
   ```bash
   cd ../../setup
   python setup_databricks.py
   ```

2. **Data Files**: Ensure the data files exist:
   - `../../data/sample_data.csv`
   - `../../data/weather_data.json`

3. **Permissions**: Your Databricks workspace must have:
   - Access to the `main` catalog
   - Write permissions to the `default` schema
   - Delta Lake enabled on the cluster

## Output

The scripts will:
- Show data previews and schemas
- Display progress through each step
- Show analytics results
- Provide table details and history
- Give troubleshooting tips if errors occur

## Troubleshooting

Common issues and solutions:

**Connection Issues**:
- Check your `~/.databrickscfg` file
- Ensure your cluster is running
- Verify your profile configuration

**File Not Found**:
- Ensure data files exist in the `data/` directory
- Check file paths are correct

**Permission Errors**:
- Verify access to `main` catalog
- Check write permissions to `default` schema
- Ensure Delta Lake is enabled

**Delta Lake Issues**:
- Confirm Delta Lake libraries are available
- Check cluster has Delta runtime
- Verify catalog permissions

## Example Output

When successful, you'll see:
```
✅ SUCCESS: All Delta operations completed successfully!
🎉 Your data is now available in the following Delta tables:
   📋 Employees: main.default.employees
   🌤️  Weather: main.default.weather
```

You can then query these tables from:
- Databricks SQL
- Databricks Notebooks
- Other Databricks Connect applications
- BI tools connected to Databricks 