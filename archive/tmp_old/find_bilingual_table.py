#!/usr/bin/env python3
"""Find bilingual table and its schema"""

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

    print("Finding tables in sandbox.rdt_knowledge schema")
    print("=" * 80)

    # List all tables
    tables = spark.sql("SHOW TABLES IN sandbox.rdt_knowledge")
    print("\nAll tables:")
    tables.show(100, truncate=False)

    # Find table with 'bilingual' in name
    print("\n" + "=" * 80)
    print("Searching for bilingual-related table...")
    all_tables = tables.collect()

    bilingual_table = None
    for row in all_tables:
        if 'bilingual' in row['tableName'].lower():
            bilingual_table = f"sandbox.rdt_knowledge.{row['tableName']}"
            print(f"\nFound: {bilingual_table}")
            break

    if bilingual_table:
        print(f"\nSchema for {bilingual_table}:")
        print("-" * 80)
        schema = spark.sql(f"DESCRIBE {bilingual_table}")
        schema.show(100, truncate=False)

        print(f"\nSample data:")
        print("-" * 80)
        sample = spark.sql(f"SELECT * FROM {bilingual_table} LIMIT 1")
        sample.show(truncate=False, vertical=True)

    spark.stop()

if __name__ == "__main__":
    main()
