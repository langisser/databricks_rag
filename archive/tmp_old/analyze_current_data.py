#!/usr/bin/env python3
"""Analyze current data in volume and table"""

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

    print("=" * 80)
    print("CURRENT DATA ANALYSIS")
    print("=" * 80)

    namespace = config['rag_config']['full_namespace']
    table_name = f"{namespace}.datax_multimodal_chunks"
    volume_path = "/Volumes/sandbox/rdt_knowledge/rdt_document"

    # Check files in volume
    print(f"\n[1] Files in volume: {volume_path}")
    try:
        files = spark.sql(f"LIST '{volume_path}'")
        files.show(truncate=False)
    except Exception as e:
        print(f"    Error: {e}")

    # Analyze table
    print(f"\n[2] Table analysis: {table_name}")
    df = spark.sql(f"SELECT * FROM {table_name}")

    print(f"\n    Total rows: {df.count()}")

    print(f"\n    Content type distribution:")
    spark.sql(f"""
        SELECT content_type, COUNT(*) as count,
               MIN(char_count) as min_chars,
               MAX(char_count) as max_chars,
               AVG(char_count) as avg_chars
        FROM {table_name}
        GROUP BY content_type
        ORDER BY count DESC
    """).show()

    print(f"\n    Section distribution:")
    spark.sql(f"""
        SELECT section, content_type, COUNT(*) as count
        FROM {table_name}
        GROUP BY section, content_type
        ORDER BY count DESC
        LIMIT 10
    """).show(truncate=False)

    # Sample chunks from each type
    print(f"\n[3] Sample text chunk:")
    spark.sql(f"""
        SELECT id, LEFT(content, 150) as content_preview, char_count, section
        FROM {table_name}
        WHERE content_type = 'text'
        LIMIT 1
    """).show(truncate=False)

    print(f"\n[4] Sample table chunk:")
    spark.sql(f"""
        SELECT id, LEFT(content, 200) as content_preview, char_count
        FROM {table_name}
        WHERE content_type = 'table'
        LIMIT 1
    """).show(truncate=False)

    # Check for table structure queries
    print(f"\n[5] Chunks containing 'tbl_bot_rspn_sbmt_log':")
    spark.sql(f"""
        SELECT id, content_type, LEFT(content, 100) as preview
        FROM {table_name}
        WHERE content LIKE '%tbl_bot_rspn_sbmt_log%'
    """).show(truncate=False)

    # Check for column information
    print(f"\n[6] Chunks with column definitions:")
    spark.sql(f"""
        SELECT id, content_type, LEFT(content, 100) as preview
        FROM {table_name}
        WHERE content LIKE '%COLUMN%' OR content LIKE '%LOG_ID%'
        LIMIT 3
    """).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
