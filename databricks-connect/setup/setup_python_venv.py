"""
Databricks Connect - Python Environment Setup & Compatibility Checker

This comprehensive script:
1. Checks your current Python version compatibility
2. Scans for other available Python installations
3. Provides installation instructions for compatible Python versions
4. Guides you through virtual environment setup
5. Checks if Databricks Connect is already installed

Run this script to get everything set up for Databricks Connect.
"""

import sys
import os
import subprocess
import platform
from pathlib import Path

def check_databricks_connect_installed():
    """Check if Databricks Connect is already installed."""
    try:
        import databricks.connect
        return True, databricks.connect.__version__
    except ImportError:
        return False, None
    except Exception as e:
        return False, str(e)

def get_compatibility_info():
    """Get the full compatibility matrix."""
    return {
        (3, 12): {
            "databricks_connect": "16.4.*",
            "databricks_runtime": "16.1+ to 17.0+",
            "status": "Latest",
            "features": "All latest features, serverless compute support"
        },
        (3, 11): {
            "databricks_connect": "15.4.*",
            "databricks_runtime": "15.1 to 15.4 LTS",
            "status": "LTS (Recommended)",
            "features": "Stable LTS release, good for production"
        },
        (3, 10): {
            "databricks_connect": "14.3.*",
            "databricks_runtime": "13.3 to 14.3 LTS",
            "status": "Legacy LTS",
            "features": "Older but stable, limited newer features"
        },
        (3, 9): {
            "databricks_connect": "13.3.*",
            "databricks_runtime": "13.3 LTS",
            "status": "Legacy",
            "features": "Basic functionality, consider upgrading Python"
        }
    }

def show_compatibility_matrix():
    """Display the full compatibility matrix."""
    print("\n" + "="*70)
    print("DATABRICKS CONNECT COMPATIBILITY MATRIX")
    print("="*70)
    
    compatibility = get_compatibility_info()
    
    print(f"{'Python':<8} {'Databricks Connect':<18} {'Runtime':<15} {'Status':<12} {'Notes'}")
    print("-" * 70)
    
    for (major, minor), info in compatibility.items():
        python_ver = f"{major}.{minor}"
        db_connect = info['databricks_connect']
        runtime = info['databricks_runtime']
        status = info['status']
        
        # Truncate long runtime strings
        if len(runtime) > 14:
            runtime = runtime[:11] + "..."
            
        print(f"{python_ver:<8} {db_connect:<18} {runtime:<15} {status:<12}")
    
    print("\nRecommendation: Use Python 3.11 with Databricks Connect 15.4.* (LTS)")

def detect_system():
    """Detect the operating system."""
    system = platform.system().lower()
    return system

def check_python_installations():
    """Check for available Python installations."""
    python_versions = []
    
    # Common Python executable names
    python_names = [
        'python3.12', 'python3.11', 'python3.10',
        'python312', 'python311', 'python310',
        'python', 'python3'
    ]
    
    for name in python_names:
        try:
            result = subprocess.run([name, '--version'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                version_str = result.stdout.strip()
                # Extract version number
                version_parts = version_str.split()
                if len(version_parts) >= 2:
                    version = version_parts[1]
                    python_versions.append((name, version))
        except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
            continue
    
    return python_versions

def get_compatible_python(python_versions):
    """Find compatible Python versions from available installations."""
    compatible = []
    
    for name, version in python_versions:
        try:
            major, minor = version.split('.')[:2]
            major, minor = int(major), int(minor)
            
            if major == 3 and minor in [10, 11, 12]:
                databricks_version = {
                    10: "14.3.*",
                    11: "15.4.*", 
                    12: "16.4.*"
                }[minor]
                
                compatible.append({
                    'executable': name,
                    'version': version,
                    'python_version': f"{major}.{minor}",
                    'databricks_connect': databricks_version,
                    'recommended': minor == 11  # Python 3.11 is LTS
                })
        except (ValueError, IndexError):
            continue
    
    return compatible

def install_python_instructions():
    """Provide instructions for installing compatible Python versions."""
    system = detect_system()
    
    print("\n" + "="*60)
    print("PYTHON INSTALLATION INSTRUCTIONS")
    print("="*60)
    
    if system == 'darwin':  # macOS
        print("📱 macOS Installation Options:")
        print()
        print("Option 1: Using Homebrew (Recommended)")
        print("   brew install python@3.11")
        print("   # or")
        print("   brew install python@3.12")
        print()
        print("Option 2: Using pyenv")
        print("   brew install pyenv")
        print("   pyenv install 3.11.9")
        print("   pyenv global 3.11.9")
        print()
        print("Option 3: Download from python.org")
        print("   Visit: https://www.python.org/downloads/macos/")
        
    elif system == 'linux':
        print("🐧 Linux Installation Options:")
        print()
        print("Ubuntu/Debian:")
        print("   sudo apt update")
        print("   sudo apt install python3.11 python3.11-venv")
        print("   # or")
        print("   sudo apt install python3.12 python3.12-venv")
        print()
        print("CentOS/RHEL/Fedora:")
        print("   sudo dnf install python3.11 python3.11-pip")
        print("   # or")
        print("   sudo yum install python3.11 python3.11-pip")
        print()
        print("Using pyenv (any Linux):")
        print("   curl https://pyenv.run | bash")
        print("   pyenv install 3.11.9")
        print("   pyenv global 3.11.9")
        
    elif system == 'windows':
        print("🪟 Windows Installation Options:")
        print()
        print("Option 1: Download from python.org (Recommended)")
        print("   Visit: https://www.python.org/downloads/windows/")
        print("   Download Python 3.11.x or 3.12.x")
        print("   ⚠️  Make sure to check 'Add Python to PATH'")
        print()
        print("Option 2: Using Chocolatey")
        print("   choco install python311")
        print("   # or")
        print("   choco install python312")
        print()
        print("Option 3: Using Windows Store")
        print("   Search for 'Python 3.11' or 'Python 3.12' in Microsoft Store")
    
    print(f"\n💡 After installation, restart your terminal/command prompt")

def create_venv_instructions(python_executable, python_version, databricks_version):
    """Provide instructions for creating virtual environment."""
    project_root = Path(__file__).parent.parent
    
    print(f"\n" + "="*60)
    print("VIRTUAL ENVIRONMENT SETUP")
    print("="*60)
    
    print(f"Using Python {python_version} with Databricks Connect {databricks_version}")
    print()
    
    # Determine the correct venv path
    if detect_system() == 'windows':
        activate_script = "venv\\Scripts\\activate"
        python_cmd = "python"
    else:
        activate_script = "venv/bin/activate"
        python_cmd = python_executable
    
    print("Step-by-step commands:")
    print("-" * 30)
    print(f"1. Navigate to project directory:")
    print(f"   cd {project_root}")
    print()
    print(f"2. Remove existing virtual environment (if any):")
    if detect_system() == 'windows':
        print("   rmdir /s venv")
    else:
        print("   rm -rf venv")
    print()
    print(f"3. Create new virtual environment:")
    print(f"   {python_executable} -m venv venv")
    print()
    print(f"4. Activate virtual environment:")
    print(f"   source {activate_script}")
    print()
    print(f"5. Upgrade pip:")
    print(f"   {python_cmd} -m pip install --upgrade pip")
    print()
    print(f"6. Install Databricks Connect:")
    print(f"   pip uninstall pyspark  # Remove conflicts")
    print(f"   pip install \"databricks-connect=={databricks_version}\"")
    print()
    print(f"7. Install dependencies from requirements.txt:")
    print(f"   pip install -r requirements.txt")
    print()
    print(f"8. Verify installation:")
    print(f"   python setup/setup_databricks.py")

def main():
    """Main function to guide Python setup."""
    print("Databricks Connect - Python Environment Setup & Compatibility Checker")
    print("=" * 75)
    
    current_version = sys.version_info
    print(f"Current Python: {current_version.major}.{current_version.minor}.{current_version.micro}")
    
    # Check if Databricks Connect is already installed
    is_installed, version = check_databricks_connect_installed()
    if is_installed:
        print(f"📦 Databricks Connect is already installed: {version}")
        
        # Check if the installed version is compatible with current Python
        compatibility = get_compatibility_info()
        current_key = (current_version.major, current_version.minor)
        
        if current_key in compatibility:
            recommended_version = compatibility[current_key]['databricks_connect'].replace('.*', '')
            installed_major_minor = '.'.join(version.split('.')[:2])
            
            if installed_major_minor == recommended_version:
                print("✅ Installed version is compatible with your Python version!")
                print("\nYour setup is ready! Next steps:")
                print("1. Configure authentication: python setup/setup_auth.py")
                print("2. Validate connection: python setup/validate_connection.py")
                print("3. Run examples: cd examples/01_basic_operations && python basic_queries.py")
                return
            else:
                print(f"⚠️  Installed version ({version}) may not be optimal for Python {current_version.major}.{current_version.minor}")
                print(f"   Recommended: {compatibility[current_key]['databricks_connect']}")
        else:
            print(f"⚠️  Python {current_version.major}.{current_version.minor} compatibility unknown")
    else:
        print("📦 Databricks Connect is not installed")
    
    # Show compatibility matrix
    show_compatibility_matrix()
    
    # Check if current Python is compatible
    if current_version.major == 3 and current_version.minor in [10, 11, 12]:
        print(f"\n✅ Current Python {current_version.major}.{current_version.minor} is compatible!")
        databricks_versions = {10: "14.3.*", 11: "15.4.*", 12: "16.4.*"}
        databricks_version = databricks_versions[current_version.minor]
        
        # Ask user if they want to proceed with current Python or look for alternatives
        print(f"\nOptions:")
        print(f"1. Use current Python {current_version.major}.{current_version.minor} (Databricks Connect {databricks_version})")
        print(f"2. Check for other Python versions on this system")
        print(f"3. Show installation instructions for other Python versions")
        
        try:
            choice = input("Choose option (1-3, or 'q' to quit): ").strip()
            
            if choice == '1':
                create_venv_instructions("python", f"{current_version.major}.{current_version.minor}", databricks_version)
                return
            elif choice == '2':
                # Continue to scan for other versions
                pass
            elif choice == '3':
                install_python_instructions()
                return
            elif choice.lower() == 'q':
                print("Setup cancelled.")
                return
            else:
                print("Invalid choice, scanning for other Python versions...")
        except (ValueError, KeyboardInterrupt):
            print("\nScanning for other Python versions...")
    else:
        print(f"\n❌ Current Python {current_version.major}.{current_version.minor} is not compatible")
        print("Supported versions: 3.10, 3.11, 3.12")
    
    # Check for other Python installations
    print("\nScanning for other Python installations...")
    python_versions = check_python_installations()
    compatible_versions = get_compatible_python(python_versions)
    
    if compatible_versions:
        print(f"\n✅ Found {len(compatible_versions)} compatible Python installation(s):")
        print()
        
        for i, py_info in enumerate(compatible_versions, 1):
            status = " (⭐ Recommended)" if py_info['recommended'] else ""
            print(f"{i}. {py_info['executable']} - Python {py_info['version']}{status}")
            print(f"   → Databricks Connect: {py_info['databricks_connect']}")
        
        print()
        try:
            choice = input(f"Which Python would you like to use? (1-{len(compatible_versions)}, or 'q' to quit): ")
            
            if choice.lower() == 'q':
                print("Setup cancelled.")
                return
            
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(compatible_versions):
                selected = compatible_versions[choice_idx]
                create_venv_instructions(
                    selected['executable'],
                    selected['python_version'], 
                    selected['databricks_connect']
                )
            else:
                print("Invalid choice.")
        except (ValueError, KeyboardInterrupt):
            print("\nSetup cancelled.")
    
    else:
        print("\n❌ No compatible Python versions found on your system.")
        install_python_instructions()
        print(f"\n🔄 After installing a compatible Python version, run this script again:")
        print(f"   python setup/setup_python_venv.py")

if __name__ == "__main__":
    main() 