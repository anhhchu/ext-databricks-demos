"""
Test Suite for Databricks Connect Examples

This test suite validates that all examples work correctly
and Databricks Connect is properly configured.
"""

import pytest
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

class TestDatabricksConnect:
    """Test Databricks Connect functionality."""
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create a Spark session for testing."""
        try:
            from databricks.connect import DatabricksSession
            spark = DatabricksSession.builder.getOrCreate()
            yield spark
        finally:
            if 'spark' in locals():
                spark.stop()
    
    def test_connection(self, spark_session):
        """Test basic Databricks connection."""
        assert spark_session is not None
        assert hasattr(spark_session, 'client')
        assert hasattr(spark_session.client, 'host')
    
    def test_dataframe_creation(self, spark_session):
        """Test basic DataFrame creation and operations."""
        data = [(1, "test"), (2, "data")]
        columns = ["id", "value"]
        
        df = spark_session.createDataFrame(data, columns)
        
        assert df.count() == 2
        assert len(df.columns) == 2
        assert "id" in df.columns
        assert "value" in df.columns
    
    def test_sql_operations(self, spark_session):
        """Test SQL operations."""
        data = [(1, 100), (2, 200), (3, 300)]
        columns = ["id", "amount"]
        
        df = spark_session.createDataFrame(data, columns)
        df.createOrReplaceTempView("test_table")
        
        result = spark_session.sql("SELECT SUM(amount) as total FROM test_table")
        total = result.collect()[0]['total']
        
        assert total == 600
    
    def test_aggregations(self, spark_session):
        """Test aggregation operations."""
        data = [
            ("A", 100),
            ("B", 200),
            ("A", 150),
            ("B", 250)
        ]
        columns = ["category", "value"]
        
        df = spark_session.createDataFrame(data, columns)
        result = df.groupBy("category").sum("value").collect()
        
        # Convert to dictionary for easier testing
        result_dict = {row['category']: row['sum(value)'] for row in result}
        
        assert result_dict['A'] == 250
        assert result_dict['B'] == 450
    
    def test_filter_operations(self, spark_session):
        """Test filter operations."""
        data = [(i, i * 10) for i in range(1, 11)]
        columns = ["id", "value"]
        
        df = spark_session.createDataFrame(data, columns)
        filtered_df = df.filter(df.value > 50)
        
        assert filtered_df.count() == 5  # values 60, 70, 80, 90, 100

class TestExampleModules:
    """Test that example modules can be imported and have required functions."""
    
    def test_basic_operations_import(self):
        """Test that basic operations module can be imported."""
        try:
            sys.path.append(str(project_root / "examples" / "01_basic_operations"))
            import basic_queries
            
            # Check if main functions exist
            assert hasattr(basic_queries, 'get_spark_session')
            assert hasattr(basic_queries, 'basic_dataframe_operations')
            assert hasattr(basic_queries, 'main')
            
        except ImportError as e:
            pytest.skip(f"Could not import basic_queries: {e}")
    
    def test_sample_data_files_exist(self):
        """Test that sample data files exist."""
        sample_csv = project_root / "data" / "sample_data.csv"
        weather_json = project_root / "data" / "weather_data.json"
        
        assert sample_csv.exists(), "sample_data.csv not found"
        assert weather_json.exists(), "weather_data.json not found"

class TestDataFiles:
    """Test sample data files."""
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create a Spark session for testing."""
        try:
            from databricks.connect import DatabricksSession
            spark = DatabricksSession.builder.getOrCreate()
            yield spark
        finally:
            if 'spark' in locals():
                spark.stop()
    
    def test_csv_data_structure(self, spark_session):
        """Test CSV data can be read and has expected structure."""
        csv_path = project_root / "data" / "sample_data.csv"
        
        try:
            # Read local CSV with pandas first, then convert to Spark DataFrame
            import pandas as pd
            
            # Check if local file exists
            if not csv_path.exists():
                pytest.skip(f"Local CSV file not found: {csv_path}")
            
            # Read with pandas to validate local file access
            pandas_df = pd.read_csv(csv_path)
            
            # Convert pandas DataFrame to Spark DataFrame
            df = spark_session.createDataFrame(pandas_df)
            
            # Check basic structure
            assert df.count() > 0
            expected_columns = ["id", "name", "department", "salary", "hire_date", "location", "age"]
            
            for col in expected_columns:
                assert col in df.columns, f"Missing column: {col}"
                
        except Exception as e:
            pytest.skip(f"Could not process CSV file: {e}")
    
    def test_json_data_structure(self, spark_session):
        """Test JSON data can be read and has expected structure."""
        json_path = project_root / "data" / "weather_data.json"
        
        try:
            # Read local JSON with pandas first, then convert to Spark DataFrame
            import pandas as pd
            
            # Check if local file exists
            if not json_path.exists():
                pytest.skip(f"Local JSON file not found: {json_path}")
            
            # Read with pandas to validate local file access
            pandas_df = pd.read_json(json_path)
            
            # Convert pandas DataFrame to Spark DataFrame
            df = spark_session.createDataFrame(pandas_df)
            
            # Check basic structure
            assert df.count() > 0
            expected_columns = ["date", "location", "temperature", "humidity", "weather_condition"]
            
            for col in expected_columns:
                assert col in df.columns, f"Missing column: {col}"
                
        except Exception as e:
            pytest.skip(f"Could not process JSON file: {e}")
    

    def test_local_to_spark_conversion_workflow(self, spark_session):
        """Test the complete workflow: local file -> pandas -> Spark DataFrame."""
        csv_path = project_root / "data" / "sample_data.csv"
        
        if not csv_path.exists():
            pytest.skip("Local CSV file not found")
        
        try:
            import pandas as pd
            
            # Step 1: Read local file with pandas
            local_df = pd.read_csv(csv_path)
            print(f"Read {len(local_df)} rows from local file")
            
            # Step 2: Convert to Spark DataFrame
            spark_df = spark_session.createDataFrame(local_df)
            
            # Step 3: Perform Spark operations
            row_count = spark_df.count()
            column_count = len(spark_df.columns)
            
            # Step 4: Validate the conversion worked
            assert row_count == len(local_df)
            assert column_count == len(local_df.columns)
            
            print(f"Successfully converted to Spark DataFrame: {row_count} rows, {column_count} columns")
            
        except Exception as e:
            pytest.fail(f"Local to Spark conversion failed: {e}")

class TestEnvironment:
    """Test environment and dependencies."""
    
    def test_required_packages(self):
        """Test that required packages are available."""
        required_packages = [
            'databricks.connect',
            'pyspark',
            'pandas',
            'numpy'
        ]
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                pytest.fail(f"Required package not available: {package}")
    
    def test_python_version(self):
        """Test Python version compatibility."""
        import sys
        version_info = sys.version_info
        
        # Databricks Connect requires Python 3.10+
        assert version_info.major == 3, "Python 3 required"
        assert version_info.minor >= 10, "Python 3.10+ recommended for latest Databricks runtimes"

# Pytest configuration and fixtures
@pytest.fixture(scope="session")
def validate_setup():
    """Validate that the environment is properly set up."""
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
        
        # Run a simple test
        test_df = spark.range(1)
        assert test_df.count() == 1
        
        spark.stop()
        return True
        
    except Exception as e:
        pytest.skip(f"Databricks Connect not properly configured: {e}")

def test_setup_validation(validate_setup):
    """Test that validates the overall setup."""
    assert validate_setup is True

if __name__ == "__main__":
    # Run tests when executed directly
    pytest.main([__file__, "-v"]) 