#!/usr/bin/env python3
"""
Find DataX document in Volume and prepare for advanced processing
Uses Databricks Connect - no download needed
"""
import sys
import os
import json
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession

def main():
    print("Finding DataX document in Volume...")

    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Initialize Databricks session
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()

    # Volume path from Iterative 02
    volume_path = "/Volumes/sandbox/rdt_knowledge/rdt_document"

    print(f"Searching in: {volume_path}")

    # List all files
    files_result = spark.sql(f"LIST '{volume_path}'")
    files_df = files_result.toPandas()

    print(f"\nTotal files found: {len(files_df)}")

    # Find DataX document
    datax_files = files_df[files_df['name'].str.contains('DataX', case=False, na=False)]

    if len(datax_files) > 0:
        print("\n[SUCCESS] DataX documents found:")
        for idx, row in datax_files.iterrows():
            print(f"  - {row['name']}")
            print(f"    Path: {row['path']}")
            print(f"    Size: {row['size']:,} bytes")

        print("\n[READY] Document path for advanced processing:")
        target_file = datax_files.iloc[0]
        print(f"  {target_file['path']}")

    else:
        print("\n[WARNING] No DataX documents found")
        print("\nAll files in volume:")
        for idx, row in files_df.iterrows():
            print(f"  - {row['name']}")

if __name__ == "__main__":
    main()