#!/usr/bin/env python3
"""Check the bilingual index schema to get correct column names"""

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

    table_name = "sandbox.rdt_knowledge.bilingual_translation_chunks"

    print(f"Checking schema for: {table_name}")
    print("=" * 80)

    # Get schema
    df = spark.sql(f"DESCRIBE {table_name}")
    df.show(100, truncate=False)

    print("\nSample data:")
    sample = spark.sql(f"SELECT * FROM {table_name} LIMIT 1")
    sample.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
