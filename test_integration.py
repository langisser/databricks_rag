#!/usr/bin/env python3
"""
Test script to verify databricks_helper submodule integration
"""

import sys
import os

def test_submodule_import():
    """Test importing functions from the databricks_helper submodule"""
    print("Testing Databricks Helper Submodule Integration...")

    try:
        # Test importing the HTML export function
        print("  - Testing HTML export import...")
        from databricks_helper.helper_function.databricks_html_log_export import export_html_log
        print("  [OK] HTML export function imported successfully")

        # Test checking if config exists
        print("  - Testing configuration file...")
        config_path = "databricks_helper/databricks_config/config.json"
        if os.path.exists(config_path):
            print("  [OK] Configuration file found")

            # Read and validate config
            import json
            with open(config_path, 'r') as f:
                config = json.load(f)

            required_keys = ['host', 'token', 'cluster_id']
            if all(key in config['databricks'] for key in required_keys):
                print("  [OK] Configuration has required keys")
                print(f"      Host: {config['databricks']['host']}")
                print(f"      Cluster ID: {config['databricks']['cluster_id']}")
                print(f"      Token: {config['databricks']['token'][:20]}...")
            else:
                print("  [FAIL] Configuration missing required keys")
        else:
            print("  [FAIL] Configuration file not found")

        # Test demo scripts exist
        print("  - Testing demo scripts...")
        demo_files = [
            "databricks_helper/helper_function/demo/demo_live_execution.py",
            "databricks_helper/helper_function/demo/demo_html_export.py"
        ]

        for demo_file in demo_files:
            if os.path.exists(demo_file):
                print(f"  [OK] Found {os.path.basename(demo_file)}")
            else:
                print(f"  [FAIL] Missing {os.path.basename(demo_file)}")

        print("\nSubmodule Integration Test Complete!")
        return True

    except ImportError as e:
        print(f"  [FAIL] Import failed: {e}")
        return False
    except Exception as e:
        print(f"  [FAIL] Unexpected error: {e}")
        return False

def test_project_structure():
    """Test the overall project structure"""
    print("\nTesting Project Structure...")

    expected_structure = {
        "planning/": "Implementation plans and documentation",
        "planning/plan_version_01.md": "Main implementation plan",
        "databricks_helper/": "Submodule for Databricks utilities",
        "databricks_helper/helper_function/": "Core helper functions",
        "databricks_helper/databricks_config/": "Configuration management",
        "INTEGRATION.md": "Integration documentation",
        "README.md": "Project documentation",
        ".gitmodules": "Git submodule configuration"
    }

    for path, description in expected_structure.items():
        if os.path.exists(path):
            print(f"  [OK] {path} - {description}")
        else:
            print(f"  [FAIL] {path} - Missing")

    return True

def test_html_export():
    """Test HTML export functionality with actual task run ID"""
    print("\nTesting HTML Export with Task Run ID 211889835770837...")

    try:
        from databricks_helper.helper_function.databricks_html_log_export import export_html_log

        # Test with the provided run ID
        run_id = "211889835770837"
        print(f"  - Attempting to export HTML log for run ID: {run_id}")

        html_file = export_html_log(run_id)

        if html_file and os.path.exists(html_file):
            print(f"  [OK] HTML log exported successfully to: {html_file}")
            file_size = os.path.getsize(html_file) / 1024  # Size in KB
            print(f"      File size: {file_size:.1f} KB")
            return True
        else:
            print(f"  [FAIL] HTML export failed or file not found")
            return False

    except Exception as e:
        print(f"  [FAIL] HTML export error: {e}")
        return False

def show_usage_examples():
    """Show usage examples for the integrated functions"""
    print("\nUsage Examples:")
    print("""
# HTML Log Export with your task run ID
from databricks_helper.helper_function.databricks_html_log_export import export_html_log
html_file = export_html_log("211889835770837")
print(f"HTML log saved to: {html_file}")

# Live SQL Execution (requires databricks-connect)
from databricks_helper.helper_function.databricks_helper import execute_sql_query
result = execute_sql_query("SELECT 'Hello RAG!' as message")
print(result['result'])

# Configuration path
config_path = "databricks_helper/databricks_config/config.json"
""")

if __name__ == "__main__":
    print("Databricks RAG - Integration Test")
    print("=" * 50)

    # Run tests
    submodule_ok = test_submodule_import()
    structure_ok = test_project_structure()
    html_export_ok = test_html_export()

    if submodule_ok and structure_ok:
        print("\nAll integration tests passed!")
        print("Ready to implement RAG pipeline with Databricks helper functions")

        if html_export_ok:
            print("HTML export functionality verified with your task run ID")

        show_usage_examples()
    else:
        print("\nSome integration tests failed")
        print("Check the setup and configuration")
        sys.exit(1)