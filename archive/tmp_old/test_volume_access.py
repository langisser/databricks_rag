#!/usr/bin/env python3
"""
Test different methods to access Unity Catalog volume
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json

def main():
    print("Testing Volume Access Methods")
    print("=" * 40)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        print("Connected to cluster")

        volume_path = "/Volumes/sandbox/rdt_knowledge/rdt_document"

        # Method 1: Direct LIST command
        try:
            print(f"\nMethod 1: LIST command")
            result = spark.sql(f"LIST '{volume_path}'")
            files = result.collect()
            print(f"SUCCESS: Found {len(files)} items")
            for i, file_info in enumerate(files[:3]):
                print(f"  {i+1}. {file_info}")
        except Exception as e:
            print(f"  Failed: {e}")

        # Method 2: Using VOLUME syntax
        try:
            print(f"\nMethod 2: VOLUME syntax")
            result = spark.sql("LIST VOLUME sandbox.rdt_knowledge.rdt_document")
            files = result.collect()
            print(f"SUCCESS: Found {len(files)} items")
            for i, file_info in enumerate(files[:3]):
                print(f"  {i+1}. {file_info}")
        except Exception as e:
            print(f"  Failed: {e}")

        # Method 3: Check if volume exists
        try:
            print(f"\nMethod 3: Check volume info")
            spark.sql("DESCRIBE VOLUME sandbox.rdt_knowledge.rdt_document").show()
        except Exception as e:
            print(f"  Failed: {e}")

        # Method 4: Check what volumes exist
        try:
            print(f"\nMethod 4: List all volumes in schema")
            spark.sql("SHOW VOLUMES IN sandbox.rdt_knowledge").show()
        except Exception as e:
            print(f"  Failed: {e}")

        spark.stop()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    main()