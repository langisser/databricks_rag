#!/usr/bin/env python3
"""
Process your real documents with chunk strategies
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json
import time

def main():
    print("Processing Your Real Documents")
    print("=" * 50)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        namespace = config['rag_config']['full_namespace']
        volume_path = "/Volumes/sandbox/rdt_knowledge/rdt_document"

        print(f"Namespace: {namespace}")

        # Step 1: Discover your real files
        print(f"\n1. Discovering your documents...")

        files_result = spark.sql(f"LIST '{volume_path}'")
        files = files_result.collect()

        print(f"Found {len(files)} real documents:")
        for i, file_info in enumerate(files, 1):
            name = file_info['name']
            size_kb = round(file_info['size'] / 1024, 1)
            file_type = name.split('.')[-1].lower() if '.' in name else 'unknown'
            print(f"  {i}. {name} ({size_kb} KB, {file_type})")

        # Step 2: Create enhanced table for real data
        print(f"\n2. Creating table for real documents...")

        # Drop and recreate for fresh start
        spark.sql(f"DROP TABLE IF EXISTS {namespace}.real_documents_v2")

        spark.sql(f"""
            CREATE TABLE {namespace}.real_documents_v2 (
                document_id STRING,
                file_name STRING,
                file_path STRING,
                file_type STRING,
                file_size_kb DOUBLE,
                content_preview STRING,
                processing_status STRING,
                extraction_method STRING,
                created_timestamp TIMESTAMP,
                metadata MAP<STRING, STRING>
            )
            USING DELTA
        """)

        print("  Real documents table created")

        # Step 3: Process each document with appropriate strategy
        print(f"\n3. Processing documents with chunk strategies...")

        processed_docs = []

        for i, file_info in enumerate(files):
            file_name = file_info['name']
            file_path = file_info['path']
            file_size = file_info['size']
            file_type = file_name.split('.')[-1].lower() if '.' in file_name else 'unknown'

            print(f"\n  Processing: {file_name}")

            doc_id = f"real_doc_{int(time.time())}_{i+1}"

            # Determine processing strategy based on file type
            if file_type == 'docx':
                strategy = "docx_specialized"
                content_preview = f"[DOCX FILE: {file_name} - Contains structured document content]"
                extraction_method = "docx_placeholder"
                processing_status = "needs_docx_library"

                # For DOCX files, determine chunk strategy based on size
                if file_size < 100000:  # < 100KB
                    chunk_strategy = {"size": 400, "overlap": 75}
                elif file_size < 500000:  # < 500KB
                    chunk_strategy = {"size": 600, "overlap": 100}
                else:  # Large docs
                    chunk_strategy = {"size": 800, "overlap": 150}

            elif file_type == 'xlsx':
                strategy = "excel_structured"
                content_preview = f"[EXCEL FILE: {file_name} - Contains tabular data]"
                extraction_method = "excel_placeholder"
                processing_status = "needs_pandas_library"

                # For Excel files, different approach - row-based chunks
                chunk_strategy = {"type": "row_based", "rows_per_chunk": 50, "include_headers": True}

            else:
                strategy = "generic_text"
                content_preview = f"[{file_type.upper()} FILE: Unknown format]"
                extraction_method = "unknown_format"
                processing_status = "unsupported_format"
                chunk_strategy = {"size": 500, "overlap": 100}

            # Store document info
            processed_docs.append({
                "doc_id": doc_id,
                "file_name": file_name,
                "file_type": file_type,
                "size_kb": round(file_size / 1024, 1),
                "strategy": strategy,
                "chunk_strategy": chunk_strategy,
                "processing_status": processing_status
            })

            # Insert into table
            spark.sql(f"""
                INSERT INTO {namespace}.real_documents_v2 VALUES (
                    '{doc_id}',
                    '{file_name}',
                    '{file_path}',
                    '{file_type}',
                    {file_size / 1024:.2f},
                    '{content_preview}',
                    '{processing_status}',
                    '{extraction_method}',
                    current_timestamp(),
                    map(
                        'original_size_bytes', '{file_size}',
                        'processing_strategy', '{strategy}',
                        'chunk_strategy', '{str(chunk_strategy).replace("'", "")}'
                    )
                )
            """)

            print(f"    Strategy: {strategy}")
            print(f"    Chunk strategy: {chunk_strategy}")
            print(f"    Status: {processing_status}")

        # Step 4: Show chunk strategies summary
        print(f"\n4. Chunk Strategies for Your Documents:")
        print(f"   {'File':<35} {'Type':<6} {'Size':<8} {'Chunk Strategy'}")
        print(f"   {'-'*35} {'-'*6} {'-'*8} {'-'*30}")

        for doc in processed_docs:
            file_display = doc['file_name'][:32] + "..." if len(doc['file_name']) > 35 else doc['file_name']
            chunk_info = str(doc['chunk_strategy'])[:30] + "..." if len(str(doc['chunk_strategy'])) > 30 else str(doc['chunk_strategy'])
            print(f"   {file_display:<35} {doc['file_type']:<6} {doc['size_kb']:<8} {chunk_info}")

        # Step 5: Create chunks table structure for real data
        print(f"\n5. Creating enhanced chunks table...")

        spark.sql(f"DROP TABLE IF EXISTS {namespace}.real_document_chunks_v2")

        spark.sql(f"""
            CREATE TABLE {namespace}.real_document_chunks_v2 (
                chunk_id STRING,
                document_id STRING,
                chunk_index INT,
                chunk_text TEXT,
                chunk_word_count INT,
                chunk_type STRING,
                chunk_strategy_used STRING,
                semantic_density DOUBLE,
                overlap_words INT,
                processing_notes STRING,
                created_timestamp TIMESTAMP,
                metadata MAP<STRING, STRING>
            )
            USING DELTA
        """)

        print("  Enhanced chunks table created")

        # Step 6: Validation
        print(f"\n6. Validation results...")

        doc_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.real_documents_v2").collect()[0]['count']

        strategy_breakdown = spark.sql(f"""
            SELECT
                extraction_method,
                COUNT(*) as doc_count,
                ROUND(AVG(file_size_kb), 2) as avg_size_kb
            FROM {namespace}.real_documents_v2
            GROUP BY extraction_method
        """).collect()

        print(f"  Total documents processed: {doc_count}")
        print(f"  Strategy breakdown:")
        for stat in strategy_breakdown:
            print(f"    {stat['extraction_method']}: {stat['doc_count']} docs, avg {stat['avg_size_kb']} KB")

        spark.stop()

        print(f"\n" + "=" * 50)
        print("REAL DOCUMENT ANALYSIS COMPLETED")
        print("=" * 50)
        print("NEXT STEPS:")
        print("1. Install python-docx for DOCX processing")
        print("2. Install pandas for Excel processing")
        print("3. Implement actual text extraction")
        print("4. Create chunks with semantic boundaries")
        print("5. Build vector search index with real data")

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    main()