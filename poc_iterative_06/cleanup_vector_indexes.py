#!/usr/bin/env python3
"""
DELETE VECTOR INDEXES - Keep only POC05_v02 All Formats

KEEP: sandbox.rdt_knowledge.poc05_v02_all_formats_hybrid_rag_index
DELETE: All other indexes in sandbox.rdt_knowledge

IMPORTANT: Execute manually and carefully!
"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.vector_search.client import VectorSearchClient

# Load config
config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
with open(config_path, 'r') as f:
    config = json.load(f)

vs_client = VectorSearchClient(
    workspace_url=config['databricks']['host'],
    personal_access_token=config['databricks']['token'],
    disable_notice=True
)

endpoint_name = config['rag_config']['vector_search_endpoint']

# Index to KEEP
PRODUCTION_INDEX = "sandbox.rdt_knowledge.poc05_v02_all_formats_hybrid_rag_index"

print("=" * 80)
print("VECTOR INDEX CLEANUP - DELETE ALL EXCEPT POC05_v02 All Formats")
print("=" * 80)

print(f"\nKEEP (Production):")
print(f"  ✅ {PRODUCTION_INDEX}")

print(f"\nListing all indexes in sandbox.rdt_knowledge...")

# Get all indexes
try:
    # List indexes by querying the endpoint
    # Note: list_indexes() API may vary, using get_index approach instead

    # Known indexes to check and delete
    indexes_to_delete = [
        # POC05 Phase 1
        "sandbox.rdt_knowledge.poc05_hybrid_rag_index",

        # POC05_v02 DOCX only
        "sandbox.rdt_knowledge.poc05_v02_hybrid_rag_index",

        # POC03/04
        "sandbox.rdt_knowledge.poc03_multimodal_rag_index",
        "sandbox.rdt_knowledge.poc04_optimized_rag_index",

        # Early experiments (check these)
        "sandbox.rdt_knowledge.all_content_rag_index",
        "sandbox.rdt_knowledge.all_content_rag_index_v1759206287",
        "sandbox.rdt_knowledge.all_documents_index",
        "sandbox.rdt_knowledge.bilingual_translation_index_v1759213209",
        "sandbox.rdt_knowledge.datax_multimodal_index",
        "sandbox.rdt_knowledge.full_content_rag_index",
        "sandbox.rdt_knowledge.production_rag_index_v1759207394",
        "sandbox.rdt_knowledge.rag_chunk_index",
        "sandbox.rdt_knowledge.semantic_focused_index_v1759209612",
        "sandbox.rdt_knowledge.table_focused_index_v1759209612",
    ]

    print(f"\n{len(indexes_to_delete)} indexes to check and delete:")
    print("-" * 80)

    deleted_count = 0
    not_found_count = 0
    error_count = 0

    for index_name in indexes_to_delete:
        short_name = index_name.replace("sandbox.rdt_knowledge.", "")

        try:
            # Check if index exists
            index = vs_client.get_index(
                endpoint_name=endpoint_name,
                index_name=index_name
            )

            # Index exists, show confirmation
            desc = index.describe()
            status = desc.get('status', {})
            indexed_rows = status.get('indexed_row_count', 0)

            print(f"\n  [{deleted_count + 1}] {short_name}")
            print(f"      Rows: {indexed_rows:,}")
            print(f"      Status: {status.get('detailed_state', 'UNKNOWN')}")

            # Ask for confirmation
            confirm = input(f"      Delete this index? (yes/no): ").strip().lower()

            if confirm == 'yes':
                print(f"      Deleting...")
                vs_client.delete_index(
                    endpoint_name=endpoint_name,
                    index_name=index_name
                )
                print(f"      ✅ DELETED")
                deleted_count += 1
            else:
                print(f"      ⏭️  SKIPPED")

        except Exception as e:
            error_msg = str(e)
            if "RESOURCE_DOES_NOT_EXIST" in error_msg or "does not exist" in error_msg.lower():
                print(f"  ⚪ {short_name} - Not found (already deleted or never existed)")
                not_found_count += 1
            else:
                print(f"  ❌ {short_name} - Error: {error_msg[:100]}")
                error_count += 1

    # Summary
    print("\n" + "=" * 80)
    print("CLEANUP SUMMARY")
    print("=" * 80)
    print(f"\n✅ Deleted: {deleted_count} indexes")
    print(f"⚪ Not Found: {not_found_count} indexes")
    print(f"❌ Errors: {error_count} indexes")
    print(f"⏭️  Skipped: {len(indexes_to_delete) - deleted_count - not_found_count - error_count} indexes")

    # Verify production index still exists
    print(f"\n[VERIFICATION] Checking production index...")
    try:
        prod_index = vs_client.get_index(
            endpoint_name=endpoint_name,
            index_name=PRODUCTION_INDEX
        )
        desc = prod_index.describe()
        status = desc.get('status', {})

        print(f"  ✅ Production index EXISTS and is {status.get('detailed_state', 'UNKNOWN')}")
        print(f"  📊 Indexed rows: {status.get('indexed_row_count', 0):,}")
        print(f"\n🎉 Production index is safe!")

    except Exception as e:
        print(f"  ❌ ERROR: Production index not found!")
        print(f"  {e}")
        print(f"\n⚠️  WARNING: Production index may have been deleted!")

except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("CLEANUP COMPLETE")
print("=" * 80)
print(f"\nNext steps:")
print(f"1. Run the SQL commands in cleanup_commands.sql to delete tables")
print(f"2. Verify production still works:")
print(f"   - Test query on {PRODUCTION_INDEX}")
print(f"   - Check table: poc05_v02_all_formats_with_metadata")
