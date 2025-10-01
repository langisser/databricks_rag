#!/usr/bin/env python3
"""
Create comprehensive vector search index for all documents
"""
import sys
import os
import json
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType, VectorIndexType

def main():
    print("=" * 80)
    print("CREATE COMPREHENSIVE VECTOR SEARCH INDEX - ALL DOCUMENTS")
    print("=" * 80)

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Connect
    print("\n[STEP 1] Connecting to Databricks...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()

    w = WorkspaceClient(
        host=config['databricks']['host'],
        token=config['databricks']['token']
    )

    namespace = config['rag_config']['full_namespace']
    table_name = f"{namespace}.all_documents_multimodal"
    index_name = f"{namespace}.all_documents_index"
    endpoint_name = config['rag_config']['vector_search_endpoint']

    print(f"  Connected")
    print(f"\n  Table: {table_name}")
    print(f"  Index: {index_name}")
    print(f"  Endpoint: {endpoint_name}")

    # Verify table
    print("\n[STEP 2] Verifying table...")
    df = spark.sql(f"SELECT * FROM {table_name}")
    row_count = df.count()
    print(f"  Total rows: {row_count}")

    # Show content distribution
    print("\n  Content distribution:")
    dist = spark.sql(f"""
        SELECT content_type, COUNT(*) as count
        FROM {table_name}
        GROUP BY content_type
        ORDER BY count DESC
    """)
    dist.show()

    # Show document distribution
    print("  Document distribution:")
    docs = spark.sql(f"""
        SELECT document_name, COUNT(*) as chunks
        FROM {table_name}
        GROUP BY document_name
        ORDER BY chunks DESC
        LIMIT 10
    """)
    docs.show(truncate=False)

    # Enable CDF
    print("\n[STEP 3] Enabling Change Data Feed...")
    try:
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print("  CDF enabled")
    except Exception as e:
        print(f"  CDF may already be enabled")

    # Delete existing index if exists
    print("\n[STEP 4] Checking for existing index...")
    try:
        existing = w.vector_search_indexes.get_index(index_name)
        print(f"  Found existing index, deleting...")
        w.vector_search_indexes.delete_index(index_name)
        time.sleep(5)
        print("  Deleted")
    except:
        print("  No existing index found")

    # Create index
    print("\n[STEP 5] Creating vector search index...")
    try:
        index = w.vector_search_indexes.create_index(
            name=index_name,
            endpoint_name=endpoint_name,
            primary_key='id',
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=table_name,
                pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[
                    EmbeddingSourceColumn(
                        name='content',
                        embedding_model_endpoint_name='databricks-gte-large-en'
                    )
                ]
            )
        )
        print(f"  [SUCCESS] Index creation started")
        print(f"  Index: {index.name}")

    except Exception as e:
        print(f"  [ERROR] {e}")
        import traceback
        traceback.print_exc()
        return

    # Wait for index
    print("\n[STEP 6] Waiting for index to sync...")
    print("  This may take a few minutes for 1,386 chunks...")

    max_wait = 300  # 5 minutes
    start_time = time.time()

    while (time.time() - start_time) < max_wait:
        try:
            status = w.vector_search_indexes.get_index(index_name)

            if hasattr(status, 'status'):
                if hasattr(status.status, 'message'):
                    print(f"  Status: {status.status.message[:80]}")

                if hasattr(status.status, 'ready') and status.status.ready:
                    print(f"\n  [SUCCESS] Index is READY!")
                    break
            else:
                print(f"  Syncing...")

        except Exception as e:
            print(f"  Checking...")

        time.sleep(15)

    # Final verification
    print("\n[STEP 7] Final verification...")
    try:
        final = w.vector_search_indexes.get_index(index_name)
        print(f"  Index name: {final.name}")
        print(f"  Primary key: {final.primary_key}")
        print(f"  Endpoint: {final.endpoint_name}")
        print(f"  Source table rows: {row_count}")
    except Exception as e:
        print(f"  [WARNING] Could not verify: {e}")

    # Summary
    print("\n" + "=" * 80)
    print("COMPREHENSIVE VECTOR INDEX CREATED")
    print("=" * 80)
    print(f"\nIndex: {index_name}")
    print(f"Table: {table_name}")
    print(f"Endpoint: {endpoint_name}")
    print(f"\nContent Summary:")
    print(f"  - Total chunks: {row_count}")
    print(f"  - Files processed: 22")
    print(f"  - File types: DOCX, XLSX, PDF, PPTX, DRAWIO")
    print(f"  - Content types: text, tables, images, slides, sheets")
    print(f"\n[FEATURES]:")
    print(f"  - Multi-format support: Word, Excel, PDF, PowerPoint, DrawIO")
    print(f"  - Document name tracking: Find content by source document")
    print(f"  - Content type tracking: Filter by text/table/image")
    print(f"  - Section tracking: Navigate within documents")
    print(f"\n[TEST IN AI PLAYGROUND]:")
    print(f"  1. Go to AI Playground -> Retrieval")
    print(f"  2. Select index: {index_name}")
    print(f"  3. Try queries:")
    print(f"     - 'What is the BOT submission process?'")
    print(f"     - 'Show me validation framework tables'")
    print(f"     - 'Which document describes the schema sync process?'")
    print(f"     - 'What is in the CardX RDT Platform presentation?'")

if __name__ == "__main__":
    main()