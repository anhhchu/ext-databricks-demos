"""
Databricks Connect - Complete Setup Script

This script provides a comprehensive setup experience for Databricks Connect:
1. Environment variables configuration (.env file)
2. Authentication setup (OAuth or Token)
3. Connection validation and testing

Run this after setting up your Python environment and installing dependencies.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

# Load environment variables from .env file
try:
    from dotenv import load_dotenv, dotenv_values
    load_dotenv()
    print("✓ Loaded environment variables from .env file")
    DOTENV_AVAILABLE = True
except ImportError:
    print("⚠ python-dotenv not available, some features may be limited")
    DOTENV_AVAILABLE = False

class DatabricksSetup:
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.env_file = self.project_root / ".env"
        self.template_file = self.project_root / "env.template"
        
    def print_header(self, title):
        """Print a formatted header."""
        print(f"\n{'='*60}")
        print(f" {title}")
        print('='*60)
    
    def print_step(self, step_num, title):
        """Print a step header."""
        print(f"\n{'='*15} STEP {step_num}: {title} {'='*15}")
    
    # ===========================================
    # STEP 1: Environment Variables Setup
    # ===========================================
    
    def setup_environment_variables(self):
        """Set up environment variables (.env file)."""
        self.print_step(1, "ENVIRONMENT SETUP")
        
        if not DOTENV_AVAILABLE:
            print("⚠ Skipping .env setup - python-dotenv not available")
            return True
        
        # Check if template exists
        if not self.template_file.exists():
            print("✗ env.template file not found!")
            print("Creating a basic template...")
            self._create_basic_template()
        
        # Copy template to .env if needed
        if self.env_file.exists():
            print("✓ .env file already exists")
            setup_choice = input("Do you want to reconfigure your .env file? (y/N): ").strip().lower()
            if setup_choice not in ['y', 'yes']:
                return True
        else:
            print("Setting up .env file from template...")
            try:
                shutil.copy2(self.template_file, self.env_file)
                print("✓ Copied env.template to .env")
            except Exception as e:
                print(f"✗ Failed to copy template: {e}")
                return False
        
        # Skip interactive configuration - use existing .env file values
        print("✓ Using existing .env file values for authentication")
        
        # Validate that no placeholder values are being used
        placeholder_warnings = []
        oauth_conflicts = []
        env_vars_to_check = {
            'DATABRICKS_HOST': 'your-workspace-url.cloud.databricks.com',
            'DATABRICKS_TOKEN': 'your-personal-access-token',
            'DATABRICKS_CLUSTER_ID': 'your-cluster-id'
        }
        
        env_token = os.environ.get('DATABRICKS_TOKEN', '')
        using_oauth = not env_token or env_token == 'your-personal-access-token'
        
        for var_name, placeholder in env_vars_to_check.items():
            value = os.environ.get(var_name, '')
            if value == placeholder or (placeholder in value):
                placeholder_warnings.append(f"  - {var_name} still contains placeholder value")
                
                # Special handling for OAuth conflicts
                if using_oauth and var_name in ['DATABRICKS_CLUSTER_ID', 'DATABRICKS_TOKEN']:
                    oauth_conflicts.append(var_name)
        
        if placeholder_warnings:
            print("\n⚠ Warning: Found placeholder values in environment:")
            for warning in placeholder_warnings:
                print(warning)
            
            if oauth_conflicts and using_oauth:
                print("\n💡 For OAuth authentication:")
                print("   - Remove or comment out DATABRICKS_TOKEN and DATABRICKS_CLUSTER_ID from .env")
                print("   - These should be configured via 'databricks auth login' instead")
                print("   - For serverless compute: No cluster ID is needed")
                print("   - Environment variables override OAuth settings in ~/.databrickscfg")
            else:
                print("\nPlease update your .env file with actual values before proceeding.")
        
        return True
    
    def _create_basic_template(self):
        """Create a basic .env template if it doesn't exist."""
        template_content = """# Databricks Workspace Configuration
DATABRICKS_HOST=https://your-workspace-url.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_CLUSTER_ID=your-cluster-id

# Optional: Authentication Profile
DATABRICKS_PROFILE=DEFAULT

# Optional: Compute Type (serverless or cluster) - for OAuth setups
DATABRICKS_COMPUTE_TYPE=serverless

# Optional: Logging Level
SPARK_CONNECT_LOG_LEVEL=INFO

# Sample Data Paths (adjust as needed)
SAMPLE_DATA_PATH=/tmp/sample_data
DELTA_TABLE_PATH=/tmp/delta_tables

# MLflow Configuration
MLFLOW_TRACKING_URI=databricks
MLFLOW_EXPERIMENT_NAME=/Users/your-email@domain.com/databricks-connect-demo

# Project Configuration
PROJECT_NAME=databricks-connect-demo
ENV=development
"""
        try:
            with open(self.template_file, 'w') as f:
                f.write(template_content)
            print("✓ Created basic env.template")
        except Exception as e:
            print(f"✗ Failed to create template: {e}")
    
    def _interactive_env_setup(self):
        """Interactive setup of environment variables."""
        print("\nConfiguring environment variables...")
        print("Press Enter to keep current value or type new value")
        
        # Read current .env file
        env_vars = {}
        if self.env_file.exists():
            try:
                env_vars = dotenv_values(self.env_file)
            except Exception:
                pass
        
        # Interactive prompts for key variables
        variables = [
            ('DATABRICKS_HOST', 'Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)'),
            ('DATABRICKS_TOKEN', 'Personal Access Token (leave empty if using OAuth)'),
            ('DATABRICKS_CLUSTER_ID', 'Cluster ID (leave empty if using serverless)'),
            ('DATABRICKS_PROFILE', 'CLI profile name'),
            ('SPARK_CONNECT_LOG_LEVEL', 'Spark logging level (INFO, DEBUG, WARN, ERROR)')
        ]
        
        updated_vars = {}
        
        for var_name, description in variables:
            current_value = env_vars.get(var_name, '')
            if current_value:
                if var_name == 'DATABRICKS_TOKEN' and current_value and current_value != 'your-personal-access-token':
                    # Mask token for display
                    masked_value = current_value[:8] + "..." + current_value[-4:] if len(current_value) > 12 else "***"
                    prompt = f"{var_name} [{masked_value}]: "
                else:
                    prompt = f"{var_name} [{current_value}]: "
            else:
                prompt = f"{var_name}: "
            
            print(f"\n{description}")
            new_value = input(prompt).strip()
            
            if new_value:
                updated_vars[var_name] = new_value
            elif current_value:
                updated_vars[var_name] = current_value
        
        # Write updated .env file
        return self._update_env_file(updated_vars)
    
    def _update_env_file(self, updated_vars):
        """Update the .env file with new values."""
        try:
            # Read template to preserve structure and comments
            with open(self.template_file, 'r') as f:
                template_content = f.read()
            
            # Update values
            import re
            for var_name, value in updated_vars.items():
                pattern = f'^{var_name}=.*$'
                replacement = f'{var_name}={value}'
                template_content = re.sub(pattern, replacement, template_content, flags=re.MULTILINE)
            
            # Write to .env
            with open(self.env_file, 'w') as f:
                f.write(template_content)
            
            print(f"\n✓ Environment file updated: {self.env_file}")
            
            # Reload environment variables
            load_dotenv(override=True)
            
            return True
            
        except Exception as e:
            print(f"✗ Failed to update .env file: {e}")
            return False
    
    # ===========================================
    # STEP 2: Authentication Setup
    # ===========================================
    
    def setup_authentication(self):
        """Set up Databricks authentication."""
        self.print_step(2, "AUTHENTICATION SETUP")
        
        # Check if Databricks CLI is installed
        if not self._check_databricks_cli():
            print("\nPlease install Databricks CLI first:")
            print("pip install databricks-cli")
            return False
        
        # Get workspace URL from environment or user input
        env_host = os.environ.get('DATABRICKS_HOST', '')
        env_token = os.environ.get('DATABRICKS_TOKEN', '')
        env_profile = os.environ.get('DATABRICKS_PROFILE', 'DEFAULT')
        
        # Use environment variables directly
        if not env_host or env_host == 'https://your-workspace-url.cloud.databricks.com':
            print("✗ DATABRICKS_HOST not configured in .env file")
            print("Please set DATABRICKS_HOST in your .env file to your actual workspace URL")
            print(f"Example: DATABRICKS_HOST=https://e2-demo-field-eng.cloud.databricks.com")
            return False
        
        workspace_url = env_host
        print(f"\n✓ Using workspace URL from environment: {workspace_url}")
        
        if not workspace_url.startswith('https://'):
            workspace_url = f"https://{workspace_url}"
        
        # Use token authentication if available, otherwise OAuth
        if env_token and env_token != 'your-personal-access-token':
            print(f"✓ Using Personal Access Token from environment with profile '{env_profile}'")
            success = self._setup_token_auth(workspace_url, env_token, env_profile)
        else:
            print("✓ No valid token found in environment, using OAuth authentication")
            print("  ℹ Note: OAuth settings are stored in ~/.databrickscfg")
            print("  ℹ Environment variables will override OAuth settings if present")
            print("  ℹ You'll choose between serverless or cluster-based compute")
            success = self._setup_oauth_auth(workspace_url)
        
        if success:
            print("\n✓ Authentication setup completed!")
            self._list_profiles()
        
        return success
    
    def _check_databricks_cli(self):
        """Check if Databricks CLI is installed."""
        try:
            result = subprocess.run(['databricks', '--version'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✓ Databricks CLI is installed: {result.stdout.strip()}")
                return True
            else:
                print("✗ Databricks CLI is not working properly")
                return False
        except FileNotFoundError:
            print("✗ Databricks CLI is not installed")
            return False
    
    def _setup_oauth_auth(self, workspace_url: str):
        """Set up OAuth authentication with compute choice."""
        print(f"\nSetting up OAuth authentication for {workspace_url}")
        
        # Check for pre-configured compute preference via environment variable
        compute_preference = os.environ.get('DATABRICKS_COMPUTE_TYPE', '').lower()
        
        if compute_preference in ['serverless', '1']:
            choice = "1"
            print("✓ Using serverless compute (from DATABRICKS_COMPUTE_TYPE environment variable)")
        elif compute_preference in ['cluster', '2']:
            choice = "2"
            print("✓ Using cluster-based compute (from DATABRICKS_COMPUTE_TYPE environment variable)")
        else:
            # Ask user about compute preference
            print("\nChoose your compute option:")
            print("1. Serverless compute (recommended for most use cases)")
            print("2. Cluster-based compute (requires existing cluster)")
            print("💡 Tip: Set DATABRICKS_COMPUTE_TYPE=serverless in .env to skip this prompt")
            
            choice = input("Enter choice (1 or 2, press Enter for serverless): ").strip() or "1"
        
        print("This will open your browser for authentication...")
        
        try:
            if choice == "1":
                print("✓ Configuring for serverless compute")
                # For serverless, we don't use --configure-cluster
                cmd = ['databricks', 'auth', 'login', '--host', workspace_url]
            else:
                print("✓ Configuring for cluster-based compute")
                # This will prompt for cluster selection during auth
                cmd = ['databricks', 'auth', 'login', '--configure-cluster', '--host', workspace_url]
            
            result = subprocess.run(cmd, check=True)
            
            if choice == "1":
                print("  ℹ Serverless compute configured - no cluster ID needed")
            else:
                print("  ℹ Cluster-based compute configured - cluster ID stored in ~/.databrickscfg")
                
            return True
        except subprocess.CalledProcessError as e:
            print(f"✗ OAuth setup failed: {e}")
            return False
    
    def _setup_token_auth(self, workspace_url: str, token: str, profile_name: str = "DEFAULT"):
        """Set up token-based authentication."""
        print(f"\nSetting up token authentication for {workspace_url}")
        
        try:
            cmd = ['databricks', 'configure', '--token', '--profile', profile_name]
            process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, 
                                     stderr=subprocess.PIPE, text=True)
            
            inputs = f"{workspace_url}\n{token}\n"
            stdout, stderr = process.communicate(input=inputs)
            
            if process.returncode == 0:
                return True
            else:
                print(f"✗ Token setup failed: {stderr}")
                return False
        except Exception as e:
            print(f"✗ Token setup failed: {e}")
            return False
    
    def _list_profiles(self):
        """List available authentication profiles."""
        try:
            result = subprocess.run(['databricks', 'auth', 'profiles'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print("\nAvailable profiles:")
                print(result.stdout)
        except Exception as e:
            print(f"Error listing profiles: {e}")
    
    def _clear_placeholder_vars(self):
        """Clear placeholder environment variables and return them for restoration."""
        placeholder_vars = {}
        env_vars_to_clear = ['DATABRICKS_CLUSTER_ID', 'DATABRICKS_TOKEN']
        
        for env_var in env_vars_to_clear:
            value = os.environ.get(env_var, '')
            if value and ('your-' in value or 'template' in value or 'placeholder' in value):
                placeholder_vars[env_var] = value
                os.environ.pop(env_var, None)
                print(f"  ⚠ Temporarily removing placeholder {env_var} (interferes with OAuth)")
        
        return placeholder_vars
    
    def _restore_placeholder_vars(self, placeholder_vars):
        """Restore placeholder environment variables."""
        for env_var, value in placeholder_vars.items():
            os.environ[env_var] = value
    
    def _create_databricks_session(self, use_serverless=None):
        """Create a DatabricksSession with the appropriate configuration."""
        from databricks.connect import DatabricksSession
        
        # If use_serverless is not explicitly provided, determine it
        if use_serverless is None:
            compute_type = os.environ.get('DATABRICKS_COMPUTE_TYPE', 'serverless').lower()
            cluster_id = os.environ.get('DATABRICKS_CLUSTER_ID', '')
            
            # Use serverless if explicitly configured or no cluster ID is set
            use_serverless = (compute_type in ['serverless', '1'] or 
                            not cluster_id or 
                            'your-cluster-id' in cluster_id)
        
        if use_serverless:
            return DatabricksSession.builder.serverless(True).getOrCreate()
        else:
            return DatabricksSession.builder.getOrCreate()
    
    # ===========================================
    # STEP 3: Connection Validation
    # ===========================================
    
    def validate_connection(self):
        """Validate the Databricks Connect setup."""
        self.print_step(3, "CONNECTION VALIDATION")
        
        print("Running comprehensive validation tests...")
        
        # Clear placeholder environment variables once for all tests
        placeholder_vars = self._clear_placeholder_vars()
        
        try:
            # Test 1: Environment validation
            if not self._validate_environment():
                return False
            
            # Test 2: Basic connection
            if not self._test_databricks_connection():
                return False
            
            # Test 3: Data operations
            if not self._test_sample_operations():
                return False
            
            # Test 4: SQL operations
            if not self._test_sql_operations():
                return False
            
            print("\n🎉 All validation tests passed!")
            print("Your Databricks Connect setup is ready to use!")
            
            return True
            
        finally:
            # Restore placeholder vars
            self._restore_placeholder_vars(placeholder_vars)
    
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
    
    def _test_databricks_connection(self):
        """Test basic Databricks Connect connection."""
        print("\n→ Testing Databricks Connect connection...")
        
        try:
            # Determine connection type for logging
            compute_type = os.environ.get('DATABRICKS_COMPUTE_TYPE', 'serverless').lower()
            cluster_id = os.environ.get('DATABRICKS_CLUSTER_ID', '')
            use_serverless = (compute_type in ['serverless', '1'] or 
                            not cluster_id or 
                            'your-cluster-id' in cluster_id)
            
            if use_serverless:
                print("  ℹ Attempting serverless connection...")
                spark = self._create_databricks_session(use_serverless=True)
                print("  ✓ Using serverless compute")
            else:
                print("  ℹ Attempting cluster-based connection...")
                spark = self._create_databricks_session(use_serverless=False)
                cluster_id = getattr(spark.client, 'cluster_id', None)
                if cluster_id:
                    print(f"  ✓ Using cluster: {cluster_id}")
                else:
                    print("  ✓ Connected (cluster configuration from ~/.databrickscfg)")
            
            print(f"  ✓ Connected to: {spark.client.host}")
            print(f"  ✓ User ID: {spark.client._user_id}")
            
            # Test basic Spark operation
            df = spark.range(5)
            count = df.count()
            print(f"  ✓ Basic Spark operation successful: Created DataFrame with {count} rows")
            
            return True
            
        except Exception as e:
            print(f"  ✗ Connection test failed: {e}")
            print("  💡 Common solutions:")
            print("     - If using OAuth: Remove DATABRICKS_TOKEN and DATABRICKS_CLUSTER_ID from .env")
            print("     - If using Token: Ensure DATABRICKS_HOST and DATABRICKS_TOKEN are set correctly")
            print("     - Environment variables override OAuth settings in ~/.databrickscfg")
            print("     - For serverless: Ensure your workspace supports serverless compute")
            print("     - Set DATABRICKS_COMPUTE_TYPE=serverless in .env for serverless")
            print("     - Check that your credentials have the necessary permissions")
            return False
    
    def _test_sample_operations(self):
        """Test sample data operations."""
        print("\n→ Testing data operations...")
        
        try:
            spark = self._create_databricks_session()
            
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
    
    def _test_sql_operations(self):
        """Test SQL operations."""
        print("\n→ Testing SQL operations...")
        
        try:
            spark = self._create_databricks_session()
            
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
    
    # ===========================================
    # Main Setup Flow
    # ===========================================
    
    def run_setup(self):
        """Run the complete setup process."""
        self.print_header("DATABRICKS CONNECT SETUP")
        
        print("\nThis script will guide you through setting up Databricks Connect:")
        print("1. Environment variables configuration (.env file)")
        print("2. Authentication setup (OAuth or Token)")
        print("3. Connection validation and testing")
        print("\nMake sure you have:")
        print("- Installed dependencies (pip install -r requirements.txt)")
        print("- Access to a Databricks workspace")
        print("- Either a personal access token or ability to use OAuth")
        
        proceed = input("\nReady to start setup? (Y/n): ").strip().lower()
        if proceed in ['n', 'no']:
            print("Setup cancelled.")
            return False
        
        # Step 1: Environment setup
        if not self.setup_environment_variables():
            print("\n❌ Environment setup failed. Please fix the issues and try again.")
            return False
        
        # Step 2: Authentication setup
        if not self.setup_authentication():
            print("\n❌ Authentication setup failed. Please fix the issues and try again.")
            return False
        
        # Step 3: Validation
        if not self.validate_connection():
            print("\n❌ Connection validation failed. Please check your configuration.")
            return False
        
        # Success!
        self.print_header("SETUP COMPLETE!")
        print("\n🎉 Databricks Connect is now configured and ready to use!")
        
        # Show the correct code pattern based on configuration
        compute_type = os.environ.get('DATABRICKS_COMPUTE_TYPE', 'serverless').lower()
        if compute_type in ['serverless', '1']:
            print("\n📝 Code pattern for your applications (serverless):")
            print("   from databricks.connect import DatabricksSession")
            print("   spark = DatabricksSession.builder.serverless(True).getOrCreate()")
        else:
            print("\n📝 Code pattern for your applications (cluster-based):")
            print("   from databricks.connect import DatabricksSession")
            print("   spark = DatabricksSession.builder.getOrCreate()")
        
        print("\nNext steps:")
        print("1. Try the examples in the examples/ directory:")
        print("   cd examples/01_basic_operations")
        print("   python basic_queries.py")
        print("\n2. Run the test suite:")
        print("   ./run_tests.sh")
        print("\n3. Start building your own Spark applications!")
        
        return True

def main():
    """Main entry point."""
    setup = DatabricksSetup()
    success = setup.run_setup()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 