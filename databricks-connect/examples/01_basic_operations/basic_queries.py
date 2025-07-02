"""
Basic Queries with Databricks Connect

This example demonstrates fundamental DataFrame operations and queries
using Databricks Connect.
"""

import os
import sys
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓ Loaded environment variables from .env file")
except ImportError:
    print("⚠ python-dotenv not available, skipping .env file loading")

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

def get_spark_session():
    """Create and return a Databricks Spark session."""
    try:
        from databricks.connect import DatabricksSession
        
        # Enable eager evaluation for better interactive experience
        spark = DatabricksSession.builder.getOrCreate()
        spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
        
        print(f"✓ Connected to Databricks: {spark.client.host}")
        print(f"✓ Spark version: {spark.version}")
        
        return spark
        
    except Exception as e:
        print(f"✗ Failed to create Spark session: {e}")
        raise

def basic_dataframe_operations(spark):
    """Demonstrate basic DataFrame operations."""
    print("\n" + "="*50)
    print("BASIC DATAFRAME OPERATIONS")
    print("="*50)
    
    # Create a simple DataFrame
    data = [
        (1, "Alice", "Engineering", 85000),
        (2, "Bob", "Marketing", 65000),
        (3, "Carol", "Engineering", 95000),
        (4, "David", "Sales", 70000),
        (5, "Eve", "Engineering", 88000)
    ]
    columns = ["id", "name", "department", "salary"]
    
    df = spark.createDataFrame(data, columns)
    
    print("1. Created DataFrame:")
    df.show()
    
    print("2. DataFrame Schema:")
    df.printSchema()
    
    print("3. DataFrame Statistics:")
    df.describe().show()
    
    print("4. Row count:")
    print(f"Total rows: {df.count()}")
    
    return df

def filtering_and_aggregation(spark, df):
    """Demonstrate filtering and aggregation operations."""
    print("\n" + "="*50)
    print("FILTERING AND AGGREGATION")
    print("="*50)
    
    # Filter operations
    print("1. High salary employees (>= 80000):")
    high_salary_df = df.filter(df.salary >= 80000)
    high_salary_df.show()
    
    print("2. Engineering department:")
    engineering_df = df.filter(df.department == "Engineering")
    engineering_df.show()
    
    # Aggregation operations
    print("3. Average salary by department:")
    avg_salary_df = df.groupBy("department").avg("salary")
    avg_salary_df.show()
    
    print("4. Count by department:")
    count_df = df.groupBy("department").count()
    count_df.show()
    
    print("5. Salary statistics:")
    salary_stats = df.agg(
        {"salary": "min"},
        {"salary": "max"},
        {"salary": "avg"}
    )
    salary_stats.show()

def advanced_transformations(spark, df):
    """Demonstrate advanced DataFrame transformations."""
    print("\n" + "="*50)
    print("ADVANCED TRANSFORMATIONS")
    print("="*50)
    
    from pyspark.sql.functions import col, when, upper, round
    
    # Add derived columns
    print("1. Adding derived columns:")
    transformed_df = df.select(
        col("id"),
        upper(col("name")).alias("name_upper"),
        col("department"),
        col("salary"),
        when(col("salary") >= 80000, "High")
        .when(col("salary") >= 60000, "Medium")
        .otherwise("Low").alias("salary_category"),
        round(col("salary") / 12, 2).alias("monthly_salary")
    )
    
    transformed_df.show()
    
    # Window functions
    print("2. Ranking employees by salary within department:")
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, rank
    
    window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
    
    ranked_df = df.withColumn(
        "salary_rank", 
        row_number().over(window_spec)
    )
    
    ranked_df.show()
    
    return transformed_df

def working_with_sample_data(spark):
    """Work with sample data if available."""
    print("\n" + "="*50)
    print("WORKING WITH SAMPLE DATA")
    print("="*50)
    
    try:
        # Try to access Databricks sample data
        sample_df = spark.read.table("samples.nyctaxi.trips")
        
        print("1. Sample NYC Taxi data (first 5 rows):")
        sample_df.show(5)
        
        print("2. Sample data schema:")
        sample_df.printSchema()
        
        print("3. Total rows in sample data:")
        print(f"Total rows: {sample_df.count()}")
        
        return sample_df
        
    except Exception as e:
        print(f"⚠ Sample data not available: {e}")
        print("Creating mock taxi data instead...")
        
        # Create mock data
        mock_data = [
            ("2023-01-01 10:00:00", "2023-01-01 10:15:00", 2, 5.2, 15.50),
            ("2023-01-01 11:00:00", "2023-01-01 11:20:00", 1, 3.1, 12.00),
            ("2023-01-01 12:00:00", "2023-01-01 12:25:00", 3, 7.8, 22.50),
        ]
        columns = ["pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "fare_amount"]
        
        mock_df = spark.createDataFrame(mock_data, columns)
        mock_df.show()
        
        return mock_df

def performance_tips(spark):
    """Demonstrate performance optimization tips."""
    print("\n" + "="*50)
    print("PERFORMANCE TIPS")
    print("="*50)
    
    # Create larger dataset for demonstration
    large_data = [(i, f"User_{i}", f"Dept_{i%5}", 50000 + (i * 1000)) 
                  for i in range(1, 1001)]
    columns = ["id", "name", "department", "salary"]
    
    large_df = spark.createDataFrame(large_data, columns)
    
    print("1. Caching for repeated operations:")
    large_df.cache()
    
    # First operation (will cache the data)
    count1 = large_df.count()
    print(f"   First count: {count1}")
    
    # Second operation (will use cached data)
    count2 = large_df.filter(large_df.salary > 70000).count()
    print(f"   High salary count: {count2}")
    
    print("2. Using explain() to understand query plans:")
    large_df.filter(large_df.salary > 70000).explain(True)
    
    # Clean up cache
    large_df.unpersist()

def main():
    """Main function to run all examples."""
    print("Databricks Connect - Basic Queries Example")
    print("=" * 60)
    
    try:
        # Get Spark session
        spark = get_spark_session()
        
        # Run examples
        df = basic_dataframe_operations(spark)
        filtering_and_aggregation(spark, df)
        transformed_df = advanced_transformations(spark, df)
        sample_df = working_with_sample_data(spark)
        performance_tips(spark)
        
        print("\n" + "="*60)
        print("✓ All basic operations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Example failed: {e}")
        raise
    finally:
        # Clean up
        try:
            spark.stop()
            print("✓ Spark session closed")
        except:
            pass

if __name__ == "__main__":
    main() 