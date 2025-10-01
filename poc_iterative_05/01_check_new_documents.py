#!/usr/bin/env python3
"""
POC05_v02 - Step 1: Check New Documents in Volume
"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession


def main():
    print("=" * 80)
    print("POC05_V02 - CHECK NEW DOCUMENTS")
    print("=" * 80)

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    print("\n[1] Connecting to Databricks...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()
    print("    Connected")

    volume_path = config['rag_config']['volume_path']
    print(f"\n[2] Scanning volume: {volume_path}")

    # List files
    try:
        files_df = spark.sql(f"""
            LIST '{volume_path}'
        """)

        files = files_df.collect()

        print(f"\n[3] Found {len(files)} files:")
        print(f"\n{'Name':<60} {'Size':>15}")
        print("-" * 80)

        total_size = 0
        doc_files = []

        for file in files:
            name = file['name']
            size = file['size']

            total_size += size

            # Filter document files
            if name.lower().endswith(('.docx', '.pdf', '.xlsx', '.txt', '.doc')):
                doc_files.append(name)
                print(f"{name:<60} {size:>15,}")

        print("-" * 80)
        print(f"Total: {len(doc_files)} document files | {total_size:,} bytes")

        # Compare with previous runs
        print(f"\n[4] Comparison with previous POCs:")

        # Get previous stats
        poc04_count = config['rag_config'].get('poc04_chunk_count', 0)
        poc05_count = config['rag_config'].get('poc05_chunk_count', 0)

        print(f"    POC04: Processed from previous documents -> {poc04_count} chunks")
        print(f"    POC05: Same as POC04 -> {poc05_count} chunks")
        print(f"    POC05_v02: Will process {len(doc_files)} documents (NEW)")

        # Show new/changed files
        print(f"\n[5] Document list:")
        for i, name in enumerate(sorted(doc_files), 1):
            print(f"    [{i:2d}] {name}")

        print(f"\n[6] Next steps:")
        print(f"    - Process {len(doc_files)} documents with semantic chunking")
        print(f"    - Add bilingual metadata (Thai + English)")
        print(f"    - Create poc05_v02_chunks_with_metadata table")
        print(f"    - Add metadata columns (content_category, document_type, etc.)")
        print(f"    - Create poc05_v02_hybrid_rag_index")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    main()
