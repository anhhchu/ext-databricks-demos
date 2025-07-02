#!/bin/bash

echo "🚀 Starting Databricks Connect Test Suite"
echo "=========================================="

# Setup environment
source venv/bin/activate
export DATABRICKS_CONFIG_PROFILE=e2-demo-field-eng

# Suppress common warnings from third-party libraries
export PYTHONWARNINGS="ignore::DeprecationWarning:distutils,ignore::DeprecationWarning:pyspark,ignore::DeprecationWarning:jupyter_client"
export JUPYTER_PLATFORM_DIRS=1

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
    echo "  1. Run examples:"
    echo "     - cd examples/01_basic_operations && python basic_queries.py"
    echo "     - cd examples/02_advanced_features && python advanced_features.py" 
    echo "     - cd examples/03_data_processing && python data_processing.py"
    echo "  2. Validate connection: python setup/validate_connection.py"
    echo "  3. Start developing: Create your own Spark applications"
else
    echo ""
    echo "❌ Some tests failed. Check the output above for details."
fi 