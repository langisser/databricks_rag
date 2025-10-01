#!/usr/bin/env python3
"""Check all_documents_index schema"""

import sys
import os
import json
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession

def main():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()

    # Check if all_documents_index is a table or just an index
    print("Searching for 'all_documents' related tables...")
    print("=" * 80)

    tables = spark.sql("SHOW TABLES IN sandbox.rdt_knowledge")
    all_tables = tables.collect()

    for row in all_tables:
        if 'all_document' in row['tableName'].lower():
            table_name = f"sandbox.rdt_knowledge.{row['tableName']}"
            print(f"\nFound: {table_name}")
            print("-" * 80)

            # Show schema
            schema = spark.sql(f"DESCRIBE {table_name}")
            schema.show(100, truncate=False)

            # Show count
            count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
            print(f"\nRow count: {count}")

            # Sample
            if count > 0:
                print("\nSample row:")
                sample = spark.sql(f"SELECT * FROM {table_name} LIMIT 1")
                sample.show(truncate=False, vertical=True)

    spark.stop()

if __name__ == "__main__":
    main()
