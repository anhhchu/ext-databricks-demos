"""
Databricks Connect - Configuration Setup Script

This script provides a streamlined setup experience for Databricks Connect using .databrickscfg profiles:
1. Profile configuration (name, authentication, compute)
2. Authentication setup (PAT, OAuth M2M, or OAuth U2M)
3. Connection validation and testing

Run this after installing dependencies to configure your Databricks connection.
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import configparser

class DatabricksProfileSetup:
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.config_file = Path.home() / '.databrickscfg'
        
    def print_header(self, title):
        """Print a formatted header."""
        print(f"\n{'='*60}")
        print(f" {title}")
        print('='*60)
    
    def print_step(self, step_num, title):
        """Print a step header."""
        print(f"\n{'='*15} STEP {step_num}: {title} {'='*15}")
    
    # ===========================================
    # STEP 1: Profile Configuration
    # ===========================================
    
    def setup_profile_configuration(self):
        """Set up Databricks configuration profile."""
        self.print_step(1, "PROFILE CONFIGURATION")
        
        print("This will create or update a Databricks configuration profile in ~/.databrickscfg")
        
        # Get profile name
        profile_name = self._get_profile_name()
        
        # Get workspace URL
        workspace_url = self._get_workspace_url()
        
        # Get authentication type and details
        auth_config = self._get_authentication_config()
        
        # Get compute configuration
        compute_config = self._get_compute_config()
        
        # Create the profile configuration
        profile_config = {
            'host': workspace_url,
            **auth_config,
            **compute_config
        }
        
        # Write to .databrickscfg
        success = self._write_profile_config(profile_name, profile_config)
        
        if success:
            print(f"\n✓ Profile '{profile_name}' configured successfully!")
            self._show_profile_config(profile_name, profile_config)
            return profile_name
        else:
            print(f"\n✗ Failed to configure profile '{profile_name}'")
            return None
    
    def _get_profile_name(self):
        """Get the profile name from user."""
        print("\nEnter a name for your Databricks configuration profile.")
        print("Examples: DEFAULT, DEV, PROD, PERSONAL")
        
        while True:
            profile_name = input("Profile name: ").strip()
            if profile_name:
                # Check if profile already exists
                if self._profile_exists(profile_name):
                    overwrite = input(f"Profile '{profile_name}' already exists. Overwrite? (y/N): ").strip().lower()
                    if overwrite in ['y', 'yes']:
                        return profile_name
                    else:
                        continue
                return profile_name
            else:
                print("Please enter a valid profile name.")
    
    def _get_workspace_url(self):
        """Get the workspace URL from user."""
        print("\nEnter your Databricks workspace URL.")
        print("Example: https://my-workspace.cloud.databricks.com")
        
        while True:
            url = input("Workspace URL: ").strip()
            if url:
                if not url.startswith('https://'):
                    url = f"https://{url}"
                return url
            else:
                print("Please enter a valid workspace URL.")
    
    def _get_authentication_config(self):
        """Get authentication configuration from user."""
        print("\nChoose your authentication method:")
        print("1. Personal Access Token (PAT) - Simple token-based authentication")
        print("2. OAuth Machine-to-Machine (M2M) - Service principal with client credentials")
        print("3. OAuth User-to-Machine (U2M) - Interactive browser-based authentication")
        
        while True:
            choice = input("Enter choice (1-3): ").strip()
            
            if choice == "1":
                return self._get_pat_config()
            elif choice == "2":
                return self._get_oauth_m2m_config()
            elif choice == "3":
                return self._get_oauth_u2m_config()
            else:
                print("Please enter 1, 2, or 3.")
    
    def _get_pat_config(self):
        """Get Personal Access Token configuration."""
        print("\nPersonal Access Token Authentication")
        print("You need to generate a PAT from your Databricks workspace:")
        print("Settings > Developer > Access tokens > Generate new token")
        
        while True:
            token = input("Personal Access Token: ").strip()
            if token:
                return {'token': token}
            else:
                print("Please enter a valid token.")
    
    def _get_oauth_m2m_config(self):
        """Get OAuth M2M configuration."""
        print("\nOAuth Machine-to-Machine Authentication")
        print("You need service principal credentials:")
        print("- Client ID from your service principal")
        print("- Client secret from your service principal")
        
        while True:
            client_id = input("Client ID: ").strip()
            if not client_id:
                print("Please enter a valid client ID.")
                continue
                
            client_secret = input("Client Secret: ").strip()
            if not client_secret:
                print("Please enter a valid client secret.")
                continue
                
            return {
                'client_id': client_id,
                'client_secret': client_secret
            }
    
    def _get_oauth_u2m_config(self):
        """Get OAuth U2M configuration."""
        print("\nOAuth User-to-Machine Authentication")
        print("This uses browser-based authentication. No additional credentials needed.")
        print("You'll authenticate through your browser when connecting.")
        
        return {}  # No additional config needed for OAuth U2M
    
    def _get_compute_config(self):
        """Get compute configuration from user."""
        print("\nChoose your compute option:")
        print("1. Serverless Compute - Automatically managed compute (recommended)")
        print("2. Cluster-based Compute - Use a specific cluster")
        
        while True:
            choice = input("Enter choice (1-2): ").strip()
            
            if choice == "1":
                return {'serverless_compute_id': 'auto'}
            elif choice == "2":
                return self._get_cluster_config()
            else:
                print("Please enter 1 or 2.")
    
    def _get_cluster_config(self):
        """Get cluster configuration."""
        print("\nCluster-based Compute")
        print("Enter your cluster ID (you can find this in the cluster URL)")
        print("Example: 0123-456789-abcde012")
        
        while True:
            cluster_id = input("Cluster ID: ").strip()
            if cluster_id:
                return {'cluster_id': cluster_id}
            else:
                print("Please enter a valid cluster ID.")
    
    def _profile_exists(self, profile_name):
        """Check if a profile already exists."""
        if not self.config_file.exists():
            return False
        
        try:
            config = configparser.ConfigParser()
            config.read(self.config_file)
            return profile_name in config.sections()
        except Exception:
            return False
    
    def _write_profile_config(self, profile_name, profile_config):
        """Write profile configuration to .databrickscfg file."""
        try:
            # Read existing config or create new
            config = configparser.ConfigParser()
            if self.config_file.exists():
                config.read(self.config_file)
            
            # Add or update the profile section
            if profile_name not in config.sections():
                config.add_section(profile_name)
            
            # Set all configuration values
            for key, value in profile_config.items():
                config.set(profile_name, key, value)
            
            # Ensure directory exists
            self.config_file.parent.mkdir(exist_ok=True)
            
            # Write the configuration
            with open(self.config_file, 'w') as f:
                config.write(f)
            
            return True
            
        except Exception as e:
            print(f"Error writing configuration: {e}")
            return False
    
    def _show_profile_config(self, profile_name, profile_config):
        """Show the profile configuration."""
        print(f"\nProfile '{profile_name}' configuration:")
        for key, value in profile_config.items():
            if key in ['token', 'client_secret']:
                # Mask sensitive values
                masked_value = value[:8] + "..." + value[-4:] if len(value) > 12 else "***"
                print(f"  {key} = {masked_value}")
            else:
                print(f"  {key} = {value}")
    
    # ===========================================
    # STEP 2: Connection Validation
    # ===========================================
    
    def validate_connection(self, profile_name):
        """Validate the Databricks Connect setup using the specified profile."""
        self.print_step(2, "CONNECTION VALIDATION")
        
        print(f"Testing connection using profile '{profile_name}'...")
        
        # Test 1: Environment validation
        if not self._validate_environment():
            return False
        
        # Test 2: Basic connection
        if not self._test_databricks_connection(profile_name):
            return False
        
        # Test 3: Data operations
        if not self._test_sample_operations(profile_name):
            return False
        
        # Test 4: SQL operations
        if not self._test_sql_operations(profile_name):
            return False
        
        print("\n🎉 All validation tests passed!")
        print("Your Databricks Connect setup is ready to use!")
        
        return True
    
    def _validate_environment(self):
        """Validate the Python environment and dependencies."""
        print("\n→ Validating Python environment...")
        
        try:
            # Check Python version
            python_version = sys.version_info
            print(f"  ✓ Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
            
            if python_version < (3, 10):
                print("  ⚠ Warning: Python 3.10+ is recommended for latest Databricks runtimes")
            
            # Check required packages
            required_packages = ['databricks.connect', 'pyspark', 'pandas', 'numpy']
            
            for package in required_packages:
                try:
                    __import__(package)
                    print(f"  ✓ {package} is available")
                except ImportError:
                    print(f"  ✗ {package} is not installed")
                    return False
            
            return True
            
        except Exception as e:
            print(f"  ✗ Environment validation failed: {e}")
            return False
    
    def _test_databricks_connection(self, profile_name):
        """Test basic Databricks Connect connection."""
        print("\n→ Testing Databricks Connect connection...")
        
        try:
            from databricks.connect import DatabricksSession
            from databricks.sdk.core import Config
            
            # Create session with the specified profile
            config = Config(profile=profile_name)
            spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
            
            print(f"  ✓ Connected to: {spark.client.host}")
            print(f"  ✓ Profile: {profile_name}")
            
            # Check compute type from profile
            profile_config = self._read_profile_config(profile_name)
            if 'serverless_compute_id' in profile_config:
                print("  ✓ Using serverless compute")
            elif 'cluster_id' in profile_config:
                print(f"  ✓ Using cluster: {profile_config['cluster_id']}")
            
            # Test basic Spark operation
            df = spark.range(5)
            count = df.count()
            print(f"  ✓ Basic Spark operation successful: Created DataFrame with {count} rows")
            
            return True
            
        except Exception as e:
            print(f"  ✗ Connection test failed: {e}")
            print("  💡 Common solutions:")
            print("     - Check that your workspace URL is correct")
            print("     - Verify your authentication credentials")
            print("     - For OAuth U2M: You may need to authenticate first with 'databricks auth login'")
            print("     - For cluster-based: Ensure the cluster is running and accessible")
            print("     - Check that your credentials have the necessary permissions")
            return False
    
    def _test_sample_operations(self, profile_name):
        """Test sample data operations."""
        print("\n→ Testing data operations...")
        
        try:
            from databricks.connect import DatabricksSession
            from databricks.sdk.core import Config
            
            config = Config(profile=profile_name)
            spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
            
            # Create a test DataFrame
            data = [(1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 150.0)]
            columns = ["id", "name", "value"]
            df = spark.createDataFrame(data, columns)
            
            # Test basic operations
            count = df.count()
            avg_value = df.select("value").agg({"value": "avg"}).collect()[0][0]
            
            print(f"  ✓ DataFrame operations successful: {count} rows, avg value: {avg_value:.1f}")
            
            return True
            
        except Exception as e:
            print(f"  ✗ Data operations test failed: {e}")
            return False
    
    def _test_sql_operations(self, profile_name):
        """Test SQL operations."""
        print("\n→ Testing SQL operations...")
        
        try:
            from databricks.connect import DatabricksSession
            from databricks.sdk.core import Config
            
            config = Config(profile=profile_name)
            spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
            
            # Create a temporary view
            data = [(1, "Product A", 100.0), (2, "Product B", 200.0), (3, "Product C", 150.0)]
            columns = ["id", "name", "price"]
            df = spark.createDataFrame(data, columns)
            df.createOrReplaceTempView("test_products")
            
            # Run SQL query
            result = spark.sql("SELECT COUNT(*) as count FROM test_products WHERE price > 120")
            count = result.collect()[0]['count']
            print(f"  ✓ SQL operations successful: Query returned {count} rows")
            
            return True
            
        except Exception as e:
            print(f"  ✗ SQL operations test failed: {e}")
            return False
    
    def _read_profile_config(self, profile_name):
        """Read profile configuration from .databrickscfg."""
        try:
            config = configparser.ConfigParser()
            config.read(self.config_file)
            if profile_name in config.sections():
                return dict(config[profile_name])
            return {}
        except Exception:
            return {}
    
    # ===========================================
    # Main Setup Flow
    # ===========================================
    
    def run_setup(self):
        """Run the complete setup process."""
        self.print_header("DATABRICKS CONNECT PROFILE SETUP")
        
        print("\nThis script will guide you through setting up Databricks Connect using profiles:")
        print("1. Profile configuration (name, authentication, compute)")
        print("2. Connection validation and testing")
        print("\nMake sure you have:")
        print("- Installed dependencies (pip install -r requirements.txt)")
        print("- Access to a Databricks workspace")
        print("- Required authentication credentials")
        
        proceed = input("\nReady to start setup? (Y/n): ").strip().lower()
        if proceed in ['n', 'no']:
            print("Setup cancelled.")
            return False
        
        # Step 1: Profile configuration
        profile_name = self.setup_profile_configuration()
        if not profile_name:
            print("\n❌ Profile configuration failed. Please try again.")
            return False
        
        # Step 2: Validation
        if not self.validate_connection(profile_name):
            print("\n❌ Connection validation failed. Please check your configuration.")
            return False
        
        # Success!
        self.print_header("SETUP COMPLETE!")
        print(f"\n🎉 Databricks Connect is now configured with profile '{profile_name}'!")
        
        # Show usage examples
        profile_config = self._read_profile_config(profile_name)
        print(f"\n📝 Code pattern for your applications:")
        print("   from databricks.connect import DatabricksSession")
        print("   from databricks.sdk.core import Config")
        print(f"   config = Config(profile='{profile_name}')")
        print("   spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()")
        
        if 'serverless_compute_id' in profile_config:
            print("\n✓ Using serverless compute - no additional configuration needed")
        elif 'cluster_id' in profile_config:
            print(f"\n✓ Using cluster: {profile_config['cluster_id']}")
        
        print(f"\nYour configuration is stored in: {self.config_file}")
        
        print("\nNext steps:")
        print("1. Try the examples in the examples/ directory:")
        print("   cd examples/01_basic_operations")
        print("   python basic_queries.py <PROFILE_NAME>")
        print("\n2. Run the test suite:")
        print("   ./run_tests.sh")
        print("\n3. Start building your own Spark applications!")
        
        return True

def main():
    """Main entry point."""
    setup = DatabricksProfileSetup()
    success = setup.run_setup()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 