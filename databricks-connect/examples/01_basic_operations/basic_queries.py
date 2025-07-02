"""
Basic Queries with Databricks Connect

This example demonstrates fundamental DataFrame operations and queries
using Databricks Connect with .databrickscfg profiles.
"""

import os
import sys
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
        print("1. Run the setup script: python setup/setup_databricks.py")
        print("2. Check your ~/.databrickscfg file")
        print("3. For OAuth U2M: Run 'databricks auth login' first")
        print("4. Ensure your profile has the correct configuration")
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
    from pyspark.sql.functions import min, max, avg
    salary_stats = df.agg(
        min("salary").alias("min_salary"),
        max("salary").alias("max_salary"),
        avg("salary").alias("avg_salary")
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


def main():
    """Main function to run all examples."""
    print("Databricks Connect - Basic Queries Example")
    print("=" * 60)
    
    # Check for profile argument
    profile_name = None
    if len(sys.argv) > 1:
        profile_name = sys.argv[1]
        print(f"Using specified profile: {profile_name}")
    else:
        print("Using default profile (you can specify a profile as: python basic_queries.py PROFILE_NAME)")
    
    try:
        # Get Spark session with optional profile
        spark = get_spark_session(profile_name)
        
        # Run examples
        df = basic_dataframe_operations(spark)
        filtering_and_aggregation(spark, df)
        transformed_df = advanced_transformations(spark, df)
        sample_df = working_with_sample_data(spark)
        
        print("\n" + "="*60)
        print("✓ All basic operations completed successfully!")
        print("📝 Usage examples:")
        print("   python basic_queries.py          # Use default profile")
        print("   python basic_queries.py DEV      # Use specific profile")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Example failed: {e}")
        print("\n💡 If this is your first time, run the setup script:")
        print("   cd ../../setup")
        print("   python setup_databricks.py")
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