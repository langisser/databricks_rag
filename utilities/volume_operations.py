#!/usr/bin/env python3
"""
Quick check of existing documents in Unity Catalog volume
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json

def main():
    print("Quick Volume Check")
    print("=" * 40)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Create Spark session
        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        print("Connected to Databricks")

        # Simple volume check
        volume_path = "/Volumes/sandbox/rdt_knowledge/rdt_document"

        try:
            # Try basic listing
            result = spark.sql(f"LIST '{volume_path}'")
            files = result.collect()

            print(f"Found {len(files)} items in {volume_path}")

            for i, file_info in enumerate(files[:5]):  # Show first 5
                print(f"  {i+1}. {file_info}")

            if len(files) > 5:
                print(f"  ... and {len(files) - 5} more files")

        except Exception as e:
            print(f"Error accessing volume: {e}")

        spark.stop()
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    main()