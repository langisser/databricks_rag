#!/usr/bin/env python3
"""
POC05 - Add Metadata Using SQL (No UDFs to avoid Python version mismatch)
Adds: content_category, document_type, section_name, table_name
"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


def main():
    print("=" * 80)
    print("POC05 - ADD METADATA USING SQL")
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

    # Get POC04 table
    poc04_table = config['rag_config']['poc04_table']
    print(f"\n[2] Loading: {poc04_table}")

    df = spark.table(poc04_table)
    row_count = df.count()
    print(f"    Loaded {row_count} chunks")

    # Add metadata using CASE WHEN (SQL expressions, no UDFs)
    print(f"\n[3] Adding metadata columns using SQL expressions...")

    df_with_metadata = df.withColumn(
        'content_category',
        F.when(
            (F.col('content').contains('[TABLE]')) &
            ((F.lower(F.col('content')).contains('columns:')) |
             (F.lower(F.col('content')).contains('column name'))),
            'table_definition'
        ).when(
            F.col('content').contains('[TABLE]'),
            'table_data'
        ).when(
            (F.col('content').contains('SELECT ')) |
            (F.col('content').contains('INSERT INTO')) |
            (F.col('content').contains('def ')) |
            (F.col('content').contains('function')),
            'code_example'
        ).when(
            (F.col('content').startswith('[SECTION]')) & (F.length(F.col('content')) < 200),
            'header'
        ).otherwise('process_description')
    ).withColumn(
        'document_type',
        F.when(
            F.lower(F.col('source_document')).contains('validation'),
            'validation_guide'
        ).when(
            F.lower(F.col('source_document')).contains('framework'),
            'framework_guide'
        ).when(
            F.lower(F.col('source_document')).contains('process'),
            'process_guide'
        ).when(
            F.lower(F.col('source_document')).contains('submission'),
            'technical_spec'
        ).when(
            (F.lower(F.col('source_document')).contains('standard')) |
            (F.lower(F.col('source_document')).contains('guideline')),
            'standard_guide'
        ).when(
            F.lower(F.col('source_document')).contains('counterparty'),
            'technical_spec'
        ).otherwise('technical_spec')
    ).withColumn(
        'section_name',
        # Extract from [SECTION] marker
        F.when(
            F.col('content').contains('[SECTION]'),
            F.regexp_extract(F.col('content'), r'\[SECTION\]\s*(.+?)(?:\n|KEYWORDS:)', 1)
        ).when(
            F.col('content').contains('[TABLE]'),
            F.regexp_extract(F.col('content'), r'\[TABLE\]\s*(.+?)(?:\n|KEYWORDS:)', 1)
        ).when(
            F.col('content').startswith('##'),
            F.regexp_extract(F.col('content'), r'^##?\s*(.+?)(?:\n|$)', 1)
        ).otherwise('Unknown')
    ).withColumn(
        'table_name',
        # Extract tbl_* pattern
        F.when(
            F.lower(F.col('content')).rlike(r'\btbl_[a-z_]+\b'),
            F.lower(F.regexp_extract(F.col('content'), r'\b(tbl_[a-z_]+)\b', 1))
        ).otherwise(None)
    )

    print("    Metadata columns added")

    # Show statistics
    print(f"\n[4] Metadata statistics:")

    print(f"\n    Content Category Distribution:")
    df_with_metadata.groupBy('content_category').count().orderBy(F.col('count').desc()).show()

    print(f"\n    Document Type Distribution:")
    df_with_metadata.groupBy('document_type').count().orderBy(F.col('count').desc()).show()

    print(f"\n    Top 10 Table Names:")
    df_with_metadata.filter(F.col('table_name').isNotNull()) \
        .groupBy('table_name').count() \
        .orderBy(F.col('count').desc()) \
        .limit(10).show(truncate=False)

    # Save to new table
    poc05_table = f"{config['rag_config']['full_namespace']}.poc05_chunks_with_metadata"

    print(f"\n[5] Saving to: {poc05_table}")
    df_with_metadata.write.format("delta").mode("overwrite").saveAsTable(poc05_table)
    print(f"    Saved {row_count} chunks")

    # Verify
    print(f"\n[6] Verifying...")
    df_verify = spark.table(poc05_table)
    verify_count = df_verify.count()
    print(f"    Verified: {verify_count} rows")

    # Check table definitions
    print(f"\n[7] Table definition chunks:")
    table_def_count = df_verify.filter(F.col('content_category') == 'table_definition').count()
    print(f"    Found {table_def_count} table definition chunks")

    if table_def_count > 0:
        print(f"\n    Sample:")
        df_verify.filter(F.col('content_category') == 'table_definition') \
            .select('table_name', 'section_name', F.substring('content', 1, 200).alias('preview')) \
            .limit(5).show(truncate=False)

    # Check for tbl_bot_rspn_sbmt_log
    print(f"\n[8] Query 3 target: tbl_bot_rspn_sbmt_log")
    bot_chunks = df_verify.filter(
        (F.col('table_name') == 'tbl_bot_rspn_sbmt_log') |
        (F.lower(F.col('content')).contains('tbl_bot_rspn_sbmt_log'))
    )
    bot_count = bot_chunks.count()
    print(f"    Found {bot_count} chunks mentioning tbl_bot_rspn_sbmt_log")

    if bot_count > 0:
        print(f"\n    Categories:")
        bot_chunks.groupBy('content_category').count().show()

        table_def_bot = bot_chunks.filter(F.col('content_category') == 'table_definition').count()
        print(f"\n    Table definition chunks: {table_def_bot}")

        if table_def_bot > 0:
            print(f"\n    Preview:")
            bot_chunks.filter(F.col('content_category') == 'table_definition') \
                .select(F.substring('content', 1, 500).alias('content_preview')) \
                .show(truncate=False)

    # Update config
    print(f"\n[9] Updating configuration...")
    config['rag_config']['poc05_table'] = poc05_table
    config['rag_config']['poc05_chunk_count'] = row_count
    config['rag_config']['poc05_created_at'] = __import__('time').strftime("%Y-%m-%dT%H:%M:%S")

    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    print("    Updated")

    # Summary
    print("\n" + "=" * 80)
    print("METADATA ADDITION COMPLETE")
    print("=" * 80)
    print(f"\nTable: {poc05_table}")
    print(f"Total: {row_count} chunks")
    print(f"Table Definitions: {table_def_count}")
    print(f"tbl_bot_rspn_sbmt_log chunks: {bot_count}")
    print(f"\nMetadata Added:")
    print(f"  - content_category: table_definition, table_data, code, process, header")
    print(f"  - document_type: technical_spec, framework_guide, process_guide, etc.")
    print(f"  - section_name: Extracted section heading")
    print(f"  - table_name: Extracted tbl_* table names")
    print(f"\nNext: Create POC05 vector index with these metadata columns for filtering")

    spark.stop()


if __name__ == "__main__":
    main()
