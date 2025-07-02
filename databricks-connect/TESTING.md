# Testing Guide for Databricks Connect

This guide explains how to run and use the `tests/test_examples.py` file to validate your Databricks Connect setup.

## Quick Start

### 1. Setup Environment
```bash
# Navigate to project directory
cd databricks-connect

# Activate virtual environment
source venv/bin/activate

# Set Databricks profile
export DATABRICKS_CONFIG_PROFILE=<your-databricks-profile>
```

### 2. Run All Tests
```bash
pytest tests/test_examples.py -v
```

## Test Categories

The test suite is organized into four main categories:

### 🔗 **TestDatabricksConnect**
Tests core Spark functionality:
```bash
# Run all connection tests
pytest tests/test_examples.py::TestDatabricksConnect -v

# Individual tests
pytest tests/test_examples.py::TestDatabricksConnect::test_connection -v
pytest tests/test_examples.py::TestDatabricksConnect::test_dataframe_creation -v
pytest tests/test_examples.py::TestDatabricksConnect::test_sql_operations -v
pytest tests/test_examples.py::TestDatabricksConnect::test_aggregations -v
pytest tests/test_examples.py::TestDatabricksConnect::test_filter_operations -v
```

### 📁 **TestDataFiles**
Tests sample data access:
```bash
# Run all data file tests
pytest tests/test_examples.py::TestDataFiles -v

# Individual tests
pytest tests/test_examples.py::TestDataFiles::test_csv_data_structure -v
pytest tests/test_examples.py::TestDataFiles::test_json_data_structure -v
```

### 📝 **TestExampleModules**
Tests example script imports:
```bash
# Run module import tests
pytest tests/test_examples.py::TestExampleModules -v

# Individual tests
pytest tests/test_examples.py::TestExampleModules::test_basic_operations_import -v
pytest tests/test_examples.py::TestExampleModules::test_sample_data_files_exist -v
```

### 🐍 **TestEnvironment**
Tests Python and package compatibility:
```bash
# Run environment tests
pytest tests/test_examples.py::TestEnvironment -v

# Individual tests
pytest tests/test_examples.py::TestEnvironment::test_required_packages -v
pytest tests/test_examples.py::TestEnvironment::test_python_version -v
```

## Common Test Commands

### Basic Usage
```bash
# Run all tests with verbose output
pytest tests/test_examples.py -v

# Run tests with detailed output including print statements
pytest tests/test_examples.py -v -s

# Stop on first failure
pytest tests/test_examples.py -x

# Run only failed tests from last run
pytest tests/test_examples.py --lf
```

### Advanced Usage
```bash
# Run with coverage report
pytest tests/test_examples.py --cov=examples --cov-report=html

# Run tests in parallel (requires pytest-xdist)
pip install pytest-xdist
pytest tests/test_examples.py -n auto

# Generate JUnit XML report
pytest tests/test_examples.py --junitxml=test-results.xml

# Run with custom markers (if defined)
pytest tests/test_examples.py -m "not slow"
```

## Expected Test Results

### ✅ **Successful Run Example:**
```
======================================================= test session starts ========================================================
platform darwin -- Python 3.12.8, pytest-8.4.1, pluggy-1.6.0
collected 11 items

tests/test_examples.py::TestDatabricksConnect::test_connection PASSED                                                        [  9%]
tests/test_examples.py::TestDatabricksConnect::test_dataframe_creation PASSED                                               [ 18%]
tests/test_examples.py::TestDatabricksConnect::test_sql_operations PASSED                                                   [ 27%]
tests/test_examples.py::TestDatabricksConnect::test_aggregations PASSED                                                     [ 36%]
tests/test_examples.py::TestDatabricksConnect::test_filter_operations PASSED                                                [ 45%]
tests/test_examples.py::TestExampleModules::test_basic_operations_import PASSED                                             [ 54%]
tests/test_examples.py::TestExampleModules::test_sample_data_files_exist PASSED                                             [ 63%]
tests/test_examples.py::TestDataFiles::test_csv_data_structure PASSED                                                       [ 72%]
tests/test_examples.py::TestDataFiles::test_json_data_structure PASSED                                                      [ 81%]
tests/test_examples.py::TestEnvironment::test_required_packages PASSED                                                      [ 90%]
tests/test_examples.py::TestEnvironment::test_python_version PASSED                                                         [100%]

================================================= 11 passed in 25.43s ==================================================
```

## One-Click Test Script

Create a convenient test runner script:

```bash
# Create the script
cat > run_tests.sh << 'EOF'
#!/bin/bash

echo "🚀 Starting Databricks Connect Test Suite"
echo "=========================================="

# Setup environment
source venv/bin/activate
export DATABRICKS_CONFIG_PROFILE=<your-databricks-profile>

# Check if virtual environment is active
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "✅ Virtual environment active: $(basename $VIRTUAL_ENV)"
else
    echo "❌ Virtual environment not active"
    exit 1
fi

# Check Databricks profile
echo "📊 Using Databricks profile: $DATABRICKS_CONFIG_PROFILE"

# Run tests
echo ""
echo "🧪 Running test suite..."
pytest tests/test_examples.py -v

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 All tests passed! Your setup is ready."
    echo ""
    echo "Next steps:"
    echo "  1. Run examples: cd examples/01_basic_operations && python basic_queries.py"
    echo "  2. Validate connection: python setup/validate_connection.py"
    echo "  3. Start developing: Create your own Spark applications"
else
    echo ""
    echo "❌ Some tests failed. Check the output above for details."
fi
EOF

# Make it executable
chmod +x run_tests.sh

# Run it
./run_tests.sh
```

## Troubleshooting

### Common Issues and Solutions

#### 1. **Authentication Errors**
```bash
# Check your profiles
databricks auth profiles

# Reconfigure if needed
databricks configure --profile <your-databricks-profile>
```

#### 2. **Connection Timeouts**
```bash
# Check cluster status
databricks clusters list

# Test with serverless compute (if available)
export DATABRICKS_SERVERLESS=true
```

#### 3. **Import Errors**
```bash
# Check if packages are installed
pip list | grep databricks

# Reinstall if needed
pip install -r requirements.txt
```

#### 4. **Python Version Issues**
```bash
# Check compatibility
python setup/setup_python_venv.py
```

## Test Configuration

### Environment Variables
Set these before running tests:
```bash
export DATABRICKS_CONFIG_PROFILE=<your-databricks-profile>  # Required
export DATABRICKS_CLUSTER_ID=your-cluster-id        # Optional
export DATABRICKS_SERVERLESS=true                   # Optional for serverless
```

### Custom pytest.ini (Optional)
Create a `pytest.ini` file for consistent test configuration:
```ini
[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --tb=short --strict-markers
markers =
    slow: marks tests as slow (deselect with '-m "not slow"')
    integration: marks tests as integration tests
```

## Continuous Integration

### GitHub Actions Example
```yaml
name: Test Databricks Connect
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.12'
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    - name: Run tests
      env:
        DATABRICKS_CONFIG_PROFILE: ${{ secrets.DATABRICKS_PROFILE }}
        DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
        DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      run: pytest tests/test_examples.py -v
```

## Monitoring and Reporting

### Generate HTML Coverage Report
```bash
pytest tests/test_examples.py --cov=examples --cov-report=html
open htmlcov/index.html  # View in browser
```

### Generate Test Report
```bash
pytest tests/test_examples.py --html=test-report.html --self-contained-html
open test-report.html  # View in browser
```

---

## Quick Reference Commands

| Command | Purpose |
|---------|---------|
| `pytest tests/test_examples.py -v` | Run all tests with verbose output |
| `pytest tests/test_examples.py -x` | Stop on first failure |
| `pytest tests/test_examples.py -k "test_connection"` | Run tests matching pattern |
| `pytest tests/test_examples.py --lf` | Run only last failed tests |
| `pytest tests/test_examples.py -m "not slow"` | Skip slow tests |
| `./run_tests.sh` | Run complete test suite with setup |

Remember to always activate your virtual environment and set the Databricks profile before running tests! 