#!/usr/bin/env python3
"""
Investigate: Why is tbl_bot_rspn_sbmt_log missing from table definitions?
"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


def main():
    print("=" * 80)
    print("INVESTIGATION: Missing tbl_bot_rspn_sbmt_log")
    print("=" * 80)

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    print("\n[1] Connecting...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()

    poc05_table = config['rag_config']['poc05_table']
    df = spark.table(poc05_table)

    print(f"\n[2] Searching for tbl_bot_rspn_sbmt_log in {poc05_table}...")

    # Find all chunks mentioning it
    bot_chunks = df.filter(
        F.lower(F.col('content')).contains('tbl_bot_rspn_sbmt_log')
    )

    count = bot_chunks.count()
    print(f"    Found {count} chunks")

    if count > 0:
        print(f"\n[3] Full content of all {count} chunks:")
        for i, row in enumerate(bot_chunks.collect(), 1):
            print(f"\n{'='*80}")
            print(f"CHUNK {i}")
            print(f"{'='*80}")
            print(f"ID: {row['id']}")
            print(f"Document: {row['source_document']}")
            print(f"Category: {row['content_category']}")
            print(f"Section: {row['section_name']}")
            print(f"Table Name: {row['table_name']}")
            print(f"\nContent:")
            print("-" * 80)
            print(row['content'])
            print("-" * 80)

    # Check if LOG_ID, SUBMISSION_ID, RESPONSE_STATUS appear anywhere
    print(f"\n[4] Searching for column names (LOG_ID, SUBMISSION_ID, RESPONSE_STATUS)...")

    log_id_chunks = df.filter(F.col('content').contains('LOG_ID')).count()
    submission_id_chunks = df.filter(F.col('content').contains('SUBMISSION_ID')).count()
    response_status_chunks = df.filter(F.col('content').contains('RESPONSE_STATUS')).count()

    print(f"    LOG_ID: {log_id_chunks} chunks")
    print(f"    SUBMISSION_ID: {submission_id_chunks} chunks")
    print(f"    RESPONSE_STATUS: {response_status_chunks} chunks")

    if log_id_chunks > 0:
        print(f"\n[5] Sample chunk with LOG_ID:")
        df.filter(F.col('content').contains('LOG_ID')) \
            .select('source_document', 'content_category', F.substring('content', 1, 500)) \
            .limit(1).show(truncate=False, vertical=True)

    # Check DataX_OPM_RDT_Submission_to_BOT document (where the table should be)
    print(f"\n[6] Checking DataX_OPM_RDT_Submission_to_BOT_v1.0.docx...")
    bot_doc_chunks = df.filter(F.col('source_document').contains('DataX_OPM_RDT_Submission_to_BOT'))

    bot_doc_count = bot_doc_chunks.count()
    print(f"    Found {bot_doc_count} chunks from this document")

    if bot_doc_count > 0:
        print(f"\n    Content categories:")
        bot_doc_chunks.groupBy('content_category').count().orderBy(F.col('count').desc()).show()

        print(f"\n    Table chunks from this document:")
        tables_in_bot_doc = bot_doc_chunks.filter(
            (F.col('content_category') == 'table_definition') |
            (F.col('content_category') == 'table_data')
        )

        tables_count = tables_in_bot_doc.count()
        print(f"    Found {tables_count} table chunks")

        if tables_count > 0:
            print(f"\n    Table names found:")
            tables_in_bot_doc.groupBy('table_name').count().show(truncate=False)

            print(f"\n    Sample table chunks:")
            tables_in_bot_doc.select('section_name', F.substring('content', 1, 300)) \
                .limit(3).show(truncate=False, vertical=True)

    print(f"\n" + "="*80)
    print("CONCLUSION")
    print("="*80)
    print("""
If tbl_bot_rspn_sbmt_log is not found:
1. It may not be in the source documents
2. It may have been chunked in a way that split the table name from definition
3. Ground truth expectations may be incorrect

This explains why Query 3 fails - the table definition simply doesn't exist in the index!
""")

    spark.stop()


if __name__ == "__main__":
    main()
