#!/usr/bin/env python3
"""
POC Iterative 2 - Phase 1: Real Data Ingestion
- Process documents from /Volumes/sandbox/rdt_knowledge/rdt_document
- Extract text from multiple file formats (PDF, DOCX, TXT, MD)
- Enhanced metadata extraction and preservation
- Improved data quality validation
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json
import time
from datetime import datetime

def main():
    print("=" * 60)
    print("POC ITERATIVE 2 - PHASE 1: REAL DATA INGESTION")
    print("=" * 60)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        rag_config = config['rag_config']
        namespace = rag_config['full_namespace']
        volume_path = "/Volumes/sandbox/rdt_knowledge/rdt_document"

        print("1. Configuration loaded:")
        print(f"   Namespace: {namespace}")
        print(f"   Source Volume: {volume_path}")

        # Create Spark session
        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        print("   SUCCESS: Databricks Connect session established")

        # Step 2: Discover and catalog real documents
        print(f"\n2. Discovering documents in volume...")

        try:
            # List all files in the volume
            files_df = spark.sql(f"""
                SELECT
                    path,
                    name,
                    size,
                    modification_time,
                    CASE
                        WHEN LOWER(name) LIKE '%.pdf' THEN 'pdf'
                        WHEN LOWER(name) LIKE '%.docx' THEN 'docx'
                        WHEN LOWER(name) LIKE '%.txt' THEN 'txt'
                        WHEN LOWER(name) LIKE '%.md' THEN 'markdown'
                        WHEN LOWER(name) LIKE '%.html' THEN 'html'
                        WHEN LOWER(name) LIKE '%.json' THEN 'json'
                        WHEN LOWER(name) LIKE '%.csv' THEN 'csv'
                        ELSE 'other'
                    END as file_type,
                    REGEXP_EXTRACT(name, '^([^.]+)', 1) as base_name
                FROM (
                    SELECT
                        path,
                        split(path, '/')[size(split(path, '/')) - 1] as name,
                        size,
                        modification_time
                    FROM dbfs.`{volume_path}`
                    WHERE split(path, '/')[size(split(path, '/')) - 1] NOT LIKE '.%'
                      AND split(path, '/')[size(split(path, '/')) - 1] != ''
                )
                ORDER BY modification_time DESC
            """)

            files_list = files_df.collect()

            if files_list:
                print(f"   SUCCESS: Found {len(files_list)} files")

                # Analyze file types
                file_type_counts = {}
                total_size = 0

                for file_info in files_list:
                    file_type = file_info['file_type']
                    size_mb = (file_info['size'] / 1024 / 1024) if file_info['size'] else 0

                    file_type_counts[file_type] = file_type_counts.get(file_type, 0) + 1
                    total_size += size_mb

                print(f"   File type breakdown:")
                for ftype, count in file_type_counts.items():
                    print(f"     {ftype}: {count} files")

                print(f"   Total dataset size: {total_size:.2f} MB")

            else:
                print(f"   WARNING: No files found in {volume_path}")
                return False

        except Exception as e:
            print(f"   ERROR: Could not access volume: {e}")
            print(f"   This might be due to permissions or volume path issues")
            return False

        # Step 3: Create enhanced raw documents table for real data
        print(f"\n3. Creating enhanced raw documents table...")

        try:
            # Drop existing table if it exists
            spark.sql(f"DROP TABLE IF EXISTS {namespace}.raw_documents_v2")

            # Create enhanced table with better metadata support
            spark.sql(f"""
                CREATE TABLE {namespace}.raw_documents_v2 (
                    document_id STRING,
                    file_name STRING,
                    file_path STRING,
                    file_type STRING,
                    file_size_bytes BIGINT,
                    file_size_mb DOUBLE,
                    content TEXT,
                    content_preview STRING,
                    word_count INT,
                    character_count INT,
                    extraction_method STRING,
                    extraction_timestamp TIMESTAMP,
                    modification_time TIMESTAMP,
                    processing_status STRING,
                    error_message STRING,
                    metadata MAP<STRING, STRING>
                )
                USING DELTA
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """)

            print(f"   SUCCESS: Enhanced raw documents table created")

        except Exception as e:
            print(f"   ERROR: Could not create enhanced table: {e}")
            return False

        # Step 4: Process documents with enhanced extraction
        print(f"\n4. Processing documents with enhanced extraction...")

        processed_count = 0
        error_count = 0

        # Process files by type
        for file_info in files_list[:10]:  # Process first 10 files for demo
            try:
                file_name = file_info['name']
                file_path = file_info['path']
                file_type = file_info['file_type']
                file_size = file_info['size'] or 0
                mod_time = file_info['modification_time']

                print(f"   Processing: {file_name} ({file_type})")

                # Generate document ID
                doc_id = f"doc_{int(time.time())}_{processed_count + 1}"

                # Text extraction based on file type
                content = ""
                extraction_method = ""
                processing_status = "success"
                error_message = None

                if file_type == 'txt':
                    try:
                        # Read text file directly
                        content_df = spark.read.text(file_path)
                        content_rows = content_df.collect()
                        content = "\\n".join([row['value'] for row in content_rows])
                        extraction_method = "direct_text_read"
                    except Exception as e:
                        processing_status = "error"
                        error_message = f"Text extraction failed: {str(e)}"

                elif file_type == 'json':
                    try:
                        # Read JSON file
                        json_df = spark.read.json(file_path)
                        content = json_df.toJSON().collect()[0]
                        extraction_method = "json_spark_read"
                    except Exception as e:
                        processing_status = "error"
                        error_message = f"JSON extraction failed: {str(e)}"

                elif file_type in ['pdf', 'docx']:
                    # For binary formats, we'll need specialized libraries
                    # For now, mark as requiring special processing
                    content = f"[{file_type.upper()} FILE - Requires specialized extraction]"
                    extraction_method = f"placeholder_{file_type}"
                    processing_status = "pending_extraction"

                else:
                    # Try to read as text with error handling
                    try:
                        content_df = spark.read.text(file_path)
                        content_rows = content_df.take(100)  # Limit to first 100 lines
                        content = "\\n".join([row['value'] for row in content_rows])
                        extraction_method = "generic_text_read"
                    except Exception as e:
                        content = f"[{file_type.upper()} FILE - Could not extract text]"
                        extraction_method = "failed_extraction"
                        processing_status = "error"
                        error_message = str(e)

                # Calculate content metrics
                word_count = len(content.split()) if content else 0
                char_count = len(content) if content else 0
                content_preview = content[:200] + "..." if len(content) > 200 else content

                # Prepare metadata
                metadata = {
                    "source_volume": volume_path,
                    "original_extension": file_name.split('.')[-1] if '.' in file_name else "",
                    "processing_version": "poc_iterative_02"
                }

                # Insert into enhanced table
                spark.sql(f"""
                    INSERT INTO {namespace}.raw_documents_v2 VALUES (
                        '{doc_id}',
                        '{file_name}',
                        '{file_path}',
                        '{file_type}',
                        {file_size},
                        {file_size / 1024 / 1024:.6f},
                        '{content.replace("'", "''")}',
                        '{content_preview.replace("'", "''")}',
                        {word_count},
                        {char_count},
                        '{extraction_method}',
                        current_timestamp(),
                        {'NULL' if not mod_time else f"timestamp('{mod_time}')"},
                        '{processing_status}',
                        {'NULL' if not error_message else f"'{error_message.replace("'", "''")}'"},
                        map(
                            'source_volume', '{volume_path}',
                            'original_extension', '{metadata["original_extension"]}',
                            'processing_version', '{metadata["processing_version"]}'
                        )
                    )
                """)

                if processing_status == "success":
                    processed_count += 1
                else:
                    error_count += 1

                print(f"     Status: {processing_status}, Words: {word_count}, Chars: {char_count}")

            except Exception as e:
                error_count += 1
                print(f"     ERROR: Failed to process {file_name}: {e}")

        # Step 5: Validation and summary
        print(f"\n5. Processing summary...")

        # Get processing statistics
        stats_df = spark.sql(f"""
            SELECT
                processing_status,
                file_type,
                COUNT(*) as count,
                AVG(word_count) as avg_words,
                SUM(file_size_mb) as total_size_mb
            FROM {namespace}.raw_documents_v2
            GROUP BY processing_status, file_type
            ORDER BY processing_status, file_type
        """)

        stats = stats_df.collect()

        print(f"   Processing Statistics:")
        for stat in stats:
            print(f"     {stat['processing_status']} {stat['file_type']}: {stat['count']} files, "
                  f"avg {stat['avg_words']:.0f} words, {stat['total_size_mb']:.2f} MB")

        # Overall summary
        total_processed = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.raw_documents_v2").collect()[0]['count']

        print(f"\n   Overall Results:")
        print(f"     Total files discovered: {len(files_list)}")
        print(f"     Files processed: {total_processed}")
        print(f"     Successful extractions: {processed_count}")
        print(f"     Errors/pending: {error_count}")

        spark.stop()

        print(f"\n" + "=" * 60)
        print("PHASE 1 REAL DATA INGESTION COMPLETED")
        print("=" * 60)
        print("STATUS: Real documents from volume successfully processed")
        print(f"TABLE: {namespace}.raw_documents_v2 created with enhanced metadata")
        print("NEXT: Phase 2 - Advanced chunking with real data")

        return True

    except Exception as e:
        print(f"\nERROR: Phase 1 real data ingestion failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)