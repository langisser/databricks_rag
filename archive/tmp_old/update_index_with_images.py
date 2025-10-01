#!/usr/bin/env python3
"""
Update vector index with image information
Adds 12 extracted images to the multimodal chunks table
"""
import sys
import os
import json
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from pyspark.sql import Row

def main():
    print("=" * 80)
    print("UPDATE VECTOR INDEX WITH IMAGES")
    print("=" * 80)

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Load image data
    image_file = os.path.join(os.path.dirname(__file__), 'image_extraction_results.json')
    with open(image_file, 'r', encoding='utf-8') as f:
        images = json.load(f)

    print(f"\n[STEP 1] Loaded {len(images)} images from extraction")

    # Connect
    print("\n[STEP 2] Connecting to Databricks...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()
    print("  Connected")

    namespace = config['rag_config']['full_namespace']
    table_name = f"{namespace}.datax_multimodal_chunks"

    # Check current table
    print(f"\n[STEP 3] Checking current table...")
    current_df = spark.sql(f"SELECT * FROM {table_name}")
    current_count = current_df.count()
    print(f"  Current rows: {current_count}")

    # Show current content types
    types_df = spark.sql(f"""
        SELECT content_type, COUNT(*) as count
        FROM {table_name}
        GROUP BY content_type
    """)
    print("\n  Current content types:")
    types_df.show()

    # Prepare image rows
    print(f"\n[STEP 4] Preparing image rows...")
    rows = []
    for img in images:
        rows.append(Row(
            id=img['id'],
            content=img['content'],
            content_type=img['content_type'],
            section=f"Visual Image {img['id'].split('_')[-1]}",
            char_count=img['char_count'],
            source_document="DataX_OPM_RDT_Submission_to_BOT_v1.0.docx"
        ))

    new_df = spark.createDataFrame(rows)
    print(f"  Created {new_df.count()} image rows")

    # Append to existing table
    print(f"\n[STEP 5] Adding images to table...")
    new_df.write.format("delta").mode("append").saveAsTable(table_name)
    print("  [SUCCESS] Images added")

    # Verify
    print(f"\n[STEP 6] Verifying update...")
    updated_df = spark.sql(f"SELECT * FROM {table_name}")
    updated_count = updated_df.count()
    print(f"  New total rows: {updated_count}")
    print(f"  Added: {updated_count - current_count} rows")

    # Show updated distribution
    print("\n  Updated content types:")
    updated_types = spark.sql(f"""
        SELECT content_type, COUNT(*) as count
        FROM {table_name}
        GROUP BY content_type
        ORDER BY count DESC
    """)
    updated_types.show()

    # Show sample images
    print("\n  Sample image rows:")
    sample = spark.sql(f"""
        SELECT id, section, SUBSTRING(content, 1, 80) as preview
        FROM {table_name}
        WHERE content_type = 'image_visual'
        LIMIT 5
    """)
    sample.show(truncate=False)

    # Summary
    print("\n" + "=" * 80)
    print("UPDATE COMPLETE")
    print("=" * 80)
    print(f"\nTable: {table_name}")
    print(f"Total chunks: {updated_count}")
    print(f"  - Text: 36")
    print(f"  - Tables: 23")
    print(f"  - Images: {len(images)}")
    print(f"\n[NEXT] Vector index will auto-sync with new image data")
    print("Wait a few minutes, then test in AI Playground!")

if __name__ == "__main__":
    main()