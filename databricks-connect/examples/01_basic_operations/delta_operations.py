"""
Delta Operations with Databricks Connect

This example demonstrates reading data from CSV and JSON files
and writing them to Delta tables in the main catalog.
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

def get_spark_session(profile_name=None):
    """Create and return a Databricks Spark session using a configuration profile.
    
    Args:
        profile_name (str, optional): Name of the profile in ~/.databrickscfg. 
                                    If None, uses DEFAULT profile.
    """
    try:
        from databricks.connect import DatabricksSession
        from databricks.sdk.core import Config
        
        # Configure using profile
        if profile_name:
            print(f"✓ Using Databricks profile: {profile_name}")
            config = Config(profile=profile_name)
            spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
        else:
            # Use default configuration (DEFAULT profile or environment variables)
            print("✓ Using default Databricks configuration")
            spark = DatabricksSession.builder.getOrCreate()
        
        print(f"✓ Connected to Databricks: {spark.client.host}")
        print(f"✓ Spark version: {spark.version}")
        
        return spark
        
    except Exception as e:
        print(f"✗ Failed to create Spark session: {e}")
        print("\n💡 Troubleshooting tips:")
        print("1. Run the setup script: python ../../setup/setup_databricks.py")
        print("2. Check your ~/.databrickscfg file")
        print("3. For OAuth U2M: Run 'databricks auth login' first")
        print("4. Ensure your profile has the correct configuration")
        raise

def read_and_write_delta_tables(spark):
    """Read sample data files and write to Delta tables in main catalog."""
    print("\n" + "="*60)
    print("DELTA OPERATIONS: READING FILES AND WRITING TO DELTA TABLES")
    print("="*60)
    
    # Define file paths relative to the project root
    project_root = Path(__file__).parent.parent.parent
    csv_file_path = project_root / "data" / "employees.csv"
    json_file_path = project_root / "data" / "weather_data.json"
    
    try:
        # Read CSV file (employee data) locally using pandas
        print("📄 STEP 1: Reading employees.csv...")
        print(f"   File path: {csv_file_path}")
        
        if not csv_file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
        
        # Read CSV locally with pandas and convert to Spark DataFrame
        employee_pandas_df = pd.read_csv(csv_file_path)
        employee_df = spark.createDataFrame(employee_pandas_df)
        
        print("   Employee data preview:")
        employee_df.show(5)
        print("   Employee data schema:")
        employee_df.printSchema()
        print(f"   Total employee records: {employee_df.count()}")
        
        # Read JSON file (weather data) locally
        print("\n🌤️  STEP 2: Reading weather_data.json...")
        print(f"   File path: {json_file_path}")
        
        if not json_file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {json_file_path}")
        
        # Read JSON locally and convert to Spark DataFrame
        with open(json_file_path, 'r') as f:
            weather_data = json.load(f)
        weather_pandas_df = pd.DataFrame(weather_data)
        weather_df = spark.createDataFrame(weather_pandas_df)
        
        print("   Weather data preview:")
        weather_df.show(5)
        print("   Weather data schema:")
        weather_df.printSchema()
        print(f"   Total weather records: {weather_df.count()}")
        
        # Write employee data to Delta table
        print("\n💾 STEP 3: Writing employee data to Delta table...")
        employee_table_name = "main.default.employees"
        
        employee_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(employee_table_name)
        
        print(f"   ✓ Employee data written to: {employee_table_name}")
        
        # Write weather data to Delta table
        print("\n💾 STEP 4: Writing weather data to Delta table...")
        weather_table_name = "main.default.weather"
        
        weather_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(weather_table_name)
        
        print(f"   ✓ Weather data written to: {weather_table_name}")
        
        # Verify the tables were created
        print("\n🔍 STEP 5: Verifying Delta tables...")
        
        # Check if tables exist and read back from Delta tables
        try:
            employee_delta_df = spark.read.table(employee_table_name)
            weather_delta_df = spark.read.table(weather_table_name)
            
            print(f"   ✓ Employee table exists with {employee_delta_df.count()} rows")
            print(f"   ✓ Weather table exists with {weather_delta_df.count()} rows")
        except Exception as e:
            print(f"   ✗ Error reading Delta tables: {e}")
            raise
        
        # Show table information
        print("\n📊 STEP 6: Table information and sample data:")
        
        print("   Employee table sample:")
        employee_delta_df.show(3, truncate=False)
        
        print("   Weather table sample:")
        weather_delta_df.show(3, truncate=False)
        
        # Demonstrate some analytics queries on the Delta tables
        print("\n📈 STEP 7: Analytics queries on Delta tables:")
        
        # Query 1: Employee analytics
        print("   📊 Employee Analytics:")
        employee_analytics_query = """
        SELECT 
            department,
            COUNT(*) as employee_count,
            ROUND(AVG(salary), 2) as avg_salary,
            MIN(salary) as min_salary,
            MAX(salary) as max_salary,
            ROUND(AVG(age), 1) as avg_age
        FROM main.default.employees 
        GROUP BY department 
        ORDER BY avg_salary DESC
        """
        spark.sql(employee_analytics_query).show()
        
        # Query 2: Weather analytics
        print("   🌡️  Weather Analytics:")
        weather_analytics_query = """
        SELECT 
            location,
            COUNT(*) as record_count,
            ROUND(AVG(temperature), 1) as avg_temp_celsius,
            ROUND(AVG(humidity), 1) as avg_humidity_percent,
            COLLECT_SET(weather_condition) as weather_conditions
        FROM main.default.weather
        GROUP BY location
        ORDER BY avg_temp_celsius DESC
        """
        spark.sql(weather_analytics_query).show(truncate=False)
        
        # Query 3: Cross-table analysis (employees and weather by location)
        print("   🔗 Cross-table Analysis (Employees and Weather by Location):")
        cross_analysis_query = """
        SELECT 
            e.location,
            COUNT(DISTINCT e.id) as employee_count,
            ROUND(AVG(e.salary), 2) as avg_employee_salary,
            ROUND(AVG(w.temperature), 1) as avg_temperature,
            ROUND(AVG(w.humidity), 1) as avg_humidity
        FROM main.default.employees e
        LEFT JOIN main.default.weather w ON e.location = w.location
        GROUP BY e.location
        ORDER BY employee_count DESC
        """
        spark.sql(cross_analysis_query).show()
        
        print("\n✅ SUCCESS: All Delta operations completed successfully!")
        print("🎉 Your data is now available in the following Delta tables:")
        print(f"   📋 Employees: {employee_table_name}")
        print(f"   🌤️  Weather: {weather_table_name}")
        
        return employee_delta_df, weather_delta_df
        
    except FileNotFoundError as e:
        print(f"✗ Data file not found: {e}")
        print("💡 Make sure the data files exist in the data/ directory")
        print("   Expected files:")
        print(f"   - {csv_file_path}")
        print(f"   - {json_file_path}")
        raise
        
    except Exception as e:
        print(f"✗ Error during Delta operations: {e}")
        print("💡 Troubleshooting tips:")
        print("1. Check that your Databricks cluster has access to the main catalog")
        print("2. Verify that Delta Lake is enabled on your cluster")
        print("3. Ensure you have write permissions to the main catalog")
        print("4. Make sure your cluster is running and accessible")
        raise

def show_table_details(spark):
    """Show detailed information about the created Delta tables."""
    print("\n" + "="*60)
    print("DELTA TABLE DETAILS")
    print("="*60)
    
    tables = ["main.default.employees", "main.default.weather"]
    
    for table_name in tables:
        try:
            print(f"\n📋 Table: {table_name}")
            print("-" * 40)
            
            # Show table properties
            spark.sql(f"DESCRIBE EXTENDED {table_name}").show(truncate=False)
            
            # Show table history (Delta Lake feature)
            print(f"\n📜 Table History for {table_name}:")
            spark.sql(f"DESCRIBE HISTORY {table_name}").show(truncate=False)
            
        except Exception as e:
            print(f"   ⚠️  Could not retrieve details for {table_name}: {e}")

def main():
    """Main function to run Delta operations."""
    print("🚀 Databricks Connect - Delta Operations Example")
    print("=" * 70)
    
    # Check for profile argument
    profile_name = None
    if len(sys.argv) > 1:
        profile_name = sys.argv[1]
        print(f"Using specified profile: {profile_name}")
    else:
        print("Using default profile (you can specify a profile as: python delta_operations.py PROFILE_NAME)")
    
    try:
        # Get Spark session with optional profile
        spark = get_spark_session(profile_name)
        
        # Run Delta operations
        employee_df, weather_df = read_and_write_delta_tables(spark)
        
        # Show detailed table information
        show_table_details(spark)
        
        print("\n" + "="*70)
        print("✅ DELTA OPERATIONS COMPLETED SUCCESSFULLY!")
        print("💡 Usage examples:")
        print("   python delta_operations.py          # Use default profile")
        print("   python delta_operations.py DEV      # Use specific profile")
        print("\n🔗 You can now query these tables from:")
        print("   - Databricks SQL")
        print("   - Databricks Notebooks") 
        print("   - Other Databricks Connect applications")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Delta operations failed: {e}")
        print("\n💡 If this is your first time, run the setup script:")
        print("   cd ../../setup")
        print("   python setup_databricks.py")
        sys.exit(1)
    finally:
        # Clean up
        try:
            spark.stop()
            print("✓ Spark session closed")
        except:
            pass

if __name__ == "__main__":
    main() 