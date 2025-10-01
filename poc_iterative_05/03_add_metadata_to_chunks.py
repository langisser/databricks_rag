#!/usr/bin/env python3
"""
POC05 - Add Metadata Columns to POC04 Chunks
Adds: content_type, document_type, section_name for filtering
"""

import sys
import os
import json
import re

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def detect_content_type(content: str) -> str:
    """
    Classify chunk content type
    Returns: table_definition | table_data | code_example | process_description | header
    """
    content_lower = content.lower()

    # Check for table markers
    if '[TABLE]' in content or 'TABLE' in content[:100]:
        # Check if it's a table definition (has COLUMNS: keyword from POC04 enhancement)
        if 'COLUMNS:' in content or 'columns:' in content_lower or 'column name' in content_lower:
            return 'table_definition'
        else:
            return 'table_data'

    # Check for code examples
    if any(marker in content for marker in ['SELECT ', 'INSERT INTO', 'UPDATE ', 'CREATE TABLE', 'def ', 'class ', 'function']):
        return 'code_example'

    # Check for headers/section markers
    if content.startswith('[SECTION]') or content.startswith('##') or content.startswith('#'):
        # If very short (< 200 chars), it's likely a header
        if len(content) < 200:
            return 'header'

    # Default: process description
    return 'process_description'


def detect_document_type(filename: str, content: str) -> str:
    """
    Classify document type based on filename and content
    Returns: technical_spec | process_guide | validation_guide | framework_guide | standard_guide
    """
    filename_lower = filename.lower()

    if 'validation' in filename_lower:
        return 'validation_guide'
    elif 'framework' in filename_lower:
        return 'framework_guide'
    elif 'process' in filename_lower:
        return 'process_guide'
    elif 'submission' in filename_lower:
        return 'technical_spec'
    elif 'standard' in filename_lower or 'guideline' in filename_lower:
        return 'standard_guide'
    elif 'counterparty' in filename_lower:
        return 'technical_spec'
    else:
        return 'technical_spec'


def extract_section_name(content: str) -> str:
    """
    Extract section name from content
    Looks for [SECTION] marker or ## header
    """
    # Check for [SECTION] marker (from POC04)
    section_match = re.search(r'\[SECTION\]\s*(.+?)(?:\n|KEYWORDS:)', content)
    if section_match:
        return section_match.group(1).strip()

    # Check for markdown headers
    header_match = re.search(r'^##?\s*(.+?)(?:\n|$)', content, re.MULTILINE)
    if header_match:
        return header_match.group(1).strip()

    # Check for table markers
    table_match = re.search(r'\[TABLE\]\s*(.+?)(?:\n|KEYWORDS:)', content)
    if table_match:
        return table_match.group(1).strip()

    return 'Unknown'


def extract_table_name(content: str) -> str:
    """
    Extract table name if this is a table chunk
    Looks for tbl_* pattern or TABLE: keyword
    """
    # Look for tbl_* pattern (Thai database naming convention)
    tbl_match = re.search(r'\b(tbl_[a-z_]+)\b', content, re.IGNORECASE)
    if tbl_match:
        return tbl_match.group(1).lower()

    # Look for TABLE: keyword followed by name
    table_keyword_match = re.search(r'TABLE:\s*([^\n]+)', content)
    if table_keyword_match:
        table_name = table_keyword_match.group(1).strip()
        # Extract tbl_* if present
        tbl_in_name = re.search(r'\b(tbl_[a-z_]+)\b', table_name, re.IGNORECASE)
        if tbl_in_name:
            return tbl_in_name.group(1).lower()
        return table_name[:100]  # Limit length

    return None


def main():
    print("=" * 80)
    print("POC05 - ADD METADATA COLUMNS TO POC04 CHUNKS")
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
    print(f"\n[2] Loading POC04 table: {poc04_table}")

    df = spark.table(poc04_table)
    row_count = df.count()
    print(f"    Loaded {row_count} chunks")

    # Show current schema
    print(f"\n[3] Current schema:")
    df.printSchema()

    # Register UDFs for metadata detection
    print(f"\n[4] Registering metadata detection functions...")
    detect_content_type_udf = F.udf(detect_content_type, StringType())
    detect_document_type_udf = F.udf(detect_document_type, StringType())
    extract_section_name_udf = F.udf(extract_section_name, StringType())
    extract_table_name_udf = F.udf(extract_table_name, StringType())

    # Add metadata columns
    print(f"\n[5] Adding metadata columns...")
    # Note: POC04 already has 'content_type' (text/table), we'll create 'content_category' instead
    df_with_metadata = df.withColumn(
        'content_category',
        detect_content_type_udf(F.col('content'))
    ).withColumn(
        'document_type',
        detect_document_type_udf(F.col('source_document'), F.col('content'))
    ).withColumn(
        'section_name',
        extract_section_name_udf(F.col('content'))
    ).withColumn(
        'table_name',
        extract_table_name_udf(F.col('content'))
    )

    print("    Metadata columns added")

    # Show statistics
    print(f"\n[6] Metadata statistics:")
    print(f"\n    Content Category Distribution:")
    df_with_metadata.groupBy('content_category').count().orderBy('count', ascending=False).show()

    print(f"\n    Document Type Distribution:")
    df_with_metadata.groupBy('document_type').count().orderBy('count', ascending=False).show()

    print(f"\n    Top 10 Table Names:")
    df_with_metadata.filter(F.col('table_name').isNotNull()) \
        .groupBy('table_name').count() \
        .orderBy('count', ascending=False) \
        .limit(10).show(truncate=False)

    # Show sample with metadata
    print(f"\n[7] Sample chunks with metadata:")
    df_with_metadata.select('content_category', 'document_type', 'section_name', 'table_name') \
        .limit(10).show(truncate=50)

    # Save to new table: poc05_chunks_with_metadata
    poc05_table = f"{config['rag_config']['full_namespace']}.poc05_chunks_with_metadata"

    print(f"\n[8] Saving to new table: {poc05_table}")
    df_with_metadata.write.format("delta").mode("overwrite").saveAsTable(poc05_table)
    print(f"    Saved {row_count} chunks with metadata")

    # Verify
    print(f"\n[9] Verifying new table...")
    df_verify = spark.table(poc05_table)
    verify_count = df_verify.count()
    print(f"    Verified: {verify_count} rows")

    print(f"\n    New schema:")
    df_verify.printSchema()

    # Show table_definition chunks specifically
    print(f"\n[10] Table definition chunks:")
    table_def_count = df_verify.filter(F.col('content_category') == 'table_definition').count()
    print(f"    Found {table_def_count} table definition chunks")

    if table_def_count > 0:
        print(f"\n    Sample table definitions:")
        df_verify.filter(F.col('content_category') == 'table_definition') \
            .select('table_name', 'section_name', F.substring('content', 1, 200).alias('content_preview')) \
            .limit(5).show(truncate=False)

    # Check for tbl_bot_rspn_sbmt_log specifically (Query 3 target)
    print(f"\n[11] Checking for tbl_bot_rspn_sbmt_log (Query 3 target):")
    bot_table_chunks = df_verify.filter(
        (F.col('table_name') == 'tbl_bot_rspn_sbmt_log') |
        (F.lower(F.col('content')).contains('tbl_bot_rspn_sbmt_log'))
    )
    bot_count = bot_table_chunks.count()
    print(f"    Found {bot_count} chunks mentioning tbl_bot_rspn_sbmt_log")

    if bot_count > 0:
        print(f"\n    Content categories:")
        bot_table_chunks.groupBy('content_category').count().show()

        print(f"\n    Table definition chunks for tbl_bot_rspn_sbmt_log:")
        bot_table_chunks.filter(F.col('content_category') == 'table_definition') \
            .select('section_name', F.substring('content', 1, 300).alias('preview')) \
            .show(truncate=False)

    # Update config
    print(f"\n[12] Updating configuration...")
    config['rag_config']['poc05_table'] = poc05_table
    config['rag_config']['poc05_chunk_count'] = row_count
    config['rag_config']['poc05_created_at'] = __import__('time').strftime("%Y-%m-%dT%H:%M:%S")

    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    print("    Configuration updated")

    # Summary
    print("\n" + "=" * 80)
    print("METADATA ADDITION COMPLETE")
    print("=" * 80)
    print(f"\nNew Table: {poc05_table}")
    print(f"Total Chunks: {row_count}")
    print(f"Table Definitions: {table_def_count}")
    print(f"\nMetadata Columns Added:")
    print(f"  - content_category: Classify chunk as table_definition, code, process, etc.")
    print(f"  - document_type: Classify document as technical_spec, guide, etc.")
    print(f"  - section_name: Extract section heading")
    print(f"  - table_name: Extract table name (if applicable)")
    print(f"\nNext Steps:")
    print(f"1. Enable Change Data Feed on {poc05_table}")
    print(f"2. Create POC05 vector index from this table")
    print(f"3. Test Query 3 with filter: content_category='table_definition'")
    print(f"4. Benchmark POC05 vs POC04 vs POC03")

    spark.stop()


if __name__ == "__main__":
    main()
