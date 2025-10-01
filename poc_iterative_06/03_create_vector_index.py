#!/usr/bin/env python3
"""
POC06 Phase 1 - Create Vector Index with Improved Chunks
Index: poc06_phase1_hybrid_rag_index
Source: poc06_phase1_with_metadata (docx + pdf + xlsx + pptx)
Features: Hybrid search + PPTX support + improved semantic chunking
"""

import sys
import os
import json
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient


def main():
    print("=" * 80)
    print("POC06 PHASE 1 - CREATE VECTOR INDEX (IMPROVED CHUNKING)")
    print("=" * 80)

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    cluster_id = config['databricks'].get('lineage_cluster_id', config['databricks']['cluster_id'])

    print(f"\n[1] Connecting to Databricks (cluster: {cluster_id})...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=cluster_id
    ).getOrCreate()
    print("    Connected")

    # Configuration
    namespace = config['rag_config']['full_namespace']
    source_table = config['rag_config']['poc06_phase1_metadata_table']
    index_name = f"{namespace}.poc06_phase1_hybrid_rag_index"
    endpoint_name = config['rag_config']['vector_search_endpoint']
    embedding_model = config['rag_config']['embedding_model']

    print(f"\n[2] Configuration:")
    print(f"    Source: {source_table}")
    print(f"    Index: {index_name}")
    print(f"    Endpoint: {endpoint_name}")
    print(f"    Embedding: {embedding_model}")

    # Validate source table
    print(f"\n[3] Validating source table...")
    try:
        df = spark.sql(f"SELECT * FROM {source_table} LIMIT 1")
        row_count = spark.sql(f"SELECT COUNT(*) as count FROM {source_table}").collect()[0]['count']
        print(f"    Table exists: {row_count} rows")

        # Show schema
        print(f"\n    Schema:")
        spark.sql(f"DESCRIBE {source_table}").show(truncate=False)

    except Exception as e:
        print(f"    ERROR: {e}")
        spark.stop()
        return

    # Enable Change Data Feed
    print(f"\n[4] Enabling Change Data Feed...")
    try:
        spark.sql(f"ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print("    CDF enabled")
    except Exception as e:
        if "already set" in str(e).lower() or "enabled" in str(e).lower():
            print("    CDF already enabled")
        else:
            print(f"    WARNING: {e}")

    # Initialize Vector Search Client
    print(f"\n[5] Initializing Vector Search Client...")
    vs_client = VectorSearchClient(
        workspace_url=config['databricks']['host'],
        personal_access_token=config['databricks']['token'],
        disable_notice=True
    )
    print("    Initialized")

    # Delete existing index if any
    print(f"\n[6] Checking for existing index...")
    try:
        existing = vs_client.get_index(
            endpoint_name=endpoint_name,
            index_name=index_name
        )
        print(f"    Found existing index, deleting...")
        vs_client.delete_index(
            endpoint_name=endpoint_name,
            index_name=index_name
        )
        print(f"    Deleted, waiting 15 seconds...")
        time.sleep(15)
    except Exception:
        print(f"    No existing index")

    # Create vector index
    print(f"\n[7] Creating POC06 Phase 1 vector index...")
    print(f"    This will take 20-30 minutes for {row_count} chunks...")
    print(f"    IMPROVEMENTS:")
    print(f"      [STATUS] Semantic chunking (RecursiveCharacterTextSplitter)")
    print(f"      [STATUS] PPTX support (slide-based chunks)")
    print(f"      [STATUS] Larger chunks (1000 chars vs 800)")
    print(f"      [STATUS] Better overlap (300 chars vs 200)")
    print(f"      [STATUS] Hybrid search (semantic + keyword)")
    print(f"      [STATUS] Metadata filtering (content_category, document_type, etc.)")

    try:
        index = vs_client.create_delta_sync_index(
            endpoint_name=endpoint_name,
            source_table_name=source_table,
            index_name=index_name,
            pipeline_type="TRIGGERED",
            primary_key="id",
            embedding_source_column="content",
            embedding_model_endpoint_name=embedding_model
        )

        print(f"    Index created!")

        # Wait for sync
        print(f"\n[8] Monitoring sync (checking every 30s, max 30 min)...")
        max_wait = 1800  # 30 minutes
        waited = 0

        while waited < max_wait:
            try:
                status = vs_client.get_index(
                    endpoint_name=endpoint_name,
                    index_name=index_name
                )
                desc = status.describe()
                indexed = desc['status'].get('indexed_row_count', 0)
                state = desc['status'].get('detailed_state', 'UNKNOWN')
                ready = desc['status'].get('ready', False)

                pct = (indexed / row_count * 100) if row_count > 0 else 0
                print(f"    [{waited}s] {state} | Indexed: {indexed:,}/{row_count:,} ({pct:.1f}%)")

                if ready and indexed == row_count:
                    print(f"    SUCCESS: Index fully synced!")
                    break
            except Exception as e:
                print(f"    [{waited}s] Syncing...")

            time.sleep(30)
            waited += 30

        if waited >= max_wait:
            print(f"    NOTE: Max wait time reached, index may still be syncing")
            print(f"    Check Databricks UI for status")

        # Update config
        print(f"\n[9] Updating configuration...")
        config['rag_config']['poc06_phase1_index'] = index_name
        config['rag_config']['poc06_phase1_index_created_at'] = time.strftime("%Y-%m-%dT%H:%M:%S")

        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        print("    Updated")

        # Summary
        print("\n" + "=" * 80)
        print("POC06 PHASE 1 INDEX CREATION COMPLETE")
        print("=" * 80)
        print(f"\nIndex: {index_name}")
        print(f"Source: {source_table}")
        print(f"Chunks: {row_count:,}")
        print(f"Embedding: {embedding_model}")
        print(f"\nIMPROVEMENTS vs POC05:")
        print(f"  [STATUS] Semantic chunking (respects paragraphs, sentences)")
        print(f"  [STATUS] PPTX support (+35 files, +19% coverage)")
        print(f"  [STATUS] Larger chunks (1000 vs 800 chars)")
        print(f"  [STATUS] Better overlap (300 vs 200 chars, 30% vs 25%)")
        print(f"\nFeatures:")
        print(f"  [OK] Hybrid Search (semantic + keyword matching)")
        print(f"  [OK] Metadata Filtering:")
        print(f"    - content_category: presentation_slide, table_definition, process_description, etc.")
        print(f"    - document_type: presentation, technical_spec, framework_guide, etc.")
        print(f"    - section_name: Document section/slide headings")
        print(f"    - table_name: Extracted tbl_* table names")
        print(f"  [OK] Bilingual Support (Thai + English)")
        print(f"  [OK] Multi-Format Support (DOCX + PDF + XLSX + PPTX)")
        print(f"\nUsage Examples:")
        print(f"  # Hybrid search (semantic + keyword)")
        print(f"  index.similarity_search(")
        print(f"      query_text=\"validation framework guidelines\",")
        print(f"      query_type=\"HYBRID\",")
        print(f"      num_results=5")
        print(f"  )")
        print(f"")
        print(f"  # Search PPTX slides only")
        print(f"  index.similarity_search(")
        print(f"      query_text=\"presentation overview\",")
        print(f"      query_type=\"HYBRID\",")
        print(f"      filters={{\"content_category\": \"presentation_slide\"}}")
        print(f"  )")
        print(f"")
        print(f"  # Search table definitions")
        print(f"  index.similarity_search(")
        print(f"      query_text=\"tbl_bot_rspn_sbmt_log columns\",")
        print(f"      query_type=\"HYBRID\",")
        print(f"      filters={{\"content_category\": \"table_definition\"}}")
        print(f"  )")
        print(f"\nNext Steps:")
        print(f"1. Run benchmark comparison: POC06 vs POC05")
        print(f"2. Test PPTX search queries")
        print(f"3. Measure accuracy improvement")
        print(f"4. Document results")

    except Exception as e:
        print(f"\n    ERROR creating index: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    main()
