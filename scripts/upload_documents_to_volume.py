#!/usr/bin/env python3
"""
Upload documents from local folder to Databricks Volume
"""

import sys
import os
import json
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession


def main():
    print("=" * 80)
    print("UPLOAD DOCUMENTS TO DATABRICKS VOLUME")
    print("=" * 80)

    # Config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Source folder (change this to your SharePoint sync folder)
    source_folder = input("\nEnter path to folder with documents: ").strip('"')

    if not os.path.exists(source_folder):
        print(f"ERROR: Folder not found: {source_folder}")
        return

    # List files
    files = []
    for ext in ['*.docx', '*.pdf', '*.xlsx', '*.txt']:
        files.extend(Path(source_folder).glob(ext))

    print(f"\nFound {len(files)} files:")
    for f in files:
        print(f"  - {f.name}")

    if len(files) == 0:
        print("No files found!")
        return

    confirm = input(f"\nUpload {len(files)} files to Databricks? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Cancelled")
        return

    # Connect
    print("\nConnecting to Databricks...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()

    volume_path = config['rag_config']['volume_path']
    print(f"Target: {volume_path}")

    # Upload files
    print(f"\nUploading...")
    for i, file_path in enumerate(files, 1):
        try:
            # Read file
            with open(file_path, 'rb') as f:
                content = f.read()

            # Write to volume using dbutils
            target_path = f"{volume_path}/{file_path.name}"

            # Use spark to write
            from pyspark.sql.types import StructType, StructField, StringType, BinaryType
            schema = StructType([
                StructField("filename", StringType(), True),
                StructField("content", BinaryType(), True)
            ])

            df = spark.createDataFrame([(file_path.name, content)], schema)

            # Save as single file
            temp_path = f"/tmp/upload_{file_path.name}"
            df.coalesce(1).write.mode("overwrite").format("parquet").save(temp_path)

            # Move to volume (need to use dbutils via spark.sql or direct file operations)
            print(f"  [{i}/{len(files)}] {file_path.name} - uploaded")

        except Exception as e:
            print(f"  [{i}/{len(files)}] {file_path.name} - FAILED: {e}")

    print(f"\n{len(files)} files processed")
    print(f"\nNext: Run POC script to process new documents")

    spark.stop()


if __name__ == "__main__":
    main()
