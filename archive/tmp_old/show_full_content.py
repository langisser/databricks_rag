#!/usr/bin/env python3
"""
Show the full content that was extracted
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json

def main():
    print("Showing FULL Document Content Extracted")
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

        # Check if full_documents table exists
        try:
            full_docs = spark.sql(f"""
                SELECT
                    file_name,
                    file_type,
                    word_count,
                    content_length,
                    paragraph_count,
                    extraction_method,
                    processing_status,
                    LEFT(full_content, 500) as content_preview
                FROM {namespace}.full_documents
                ORDER BY word_count DESC
            """).collect()

            if full_docs:
                print(f"Found {len(full_docs)} documents with FULL content:")
                print()

                for i, doc in enumerate(full_docs, 1):
                    print(f"Document {i}: {doc['file_name']}")
                    print(f"  Type: {doc['file_type']}")
                    print(f"  Words: {doc['word_count']:,}")
                    print(f"  Characters: {doc['content_length']:,}")
                    print(f"  Paragraphs: {doc['paragraph_count']}")
                    print(f"  Extraction: {doc['extraction_method']}")
                    print(f"  Status: {doc['processing_status']}")
                    print(f"  Preview: {doc['content_preview']}...")
                    print()

                # Show one complete document
                largest_doc = max(full_docs, key=lambda x: x['word_count'])
                print(f"SHOWING FULL CONTENT of largest document:")
                print(f"File: {largest_doc['file_name']} ({largest_doc['word_count']:,} words)")
                print("=" * 80)

                full_content_query = spark.sql(f"""
                    SELECT full_content
                    FROM {namespace}.full_documents
                    WHERE file_name = '{largest_doc['file_name']}'
                """).collect()

                if full_content_query:
                    full_content = full_content_query[0]['full_content']
                    # Show first 2000 characters
                    print(full_content[:2000])
                    print("...")
                    print(f"[Content continues for {len(full_content)} total characters]")

            else:
                print("No full documents found in table")

        except Exception as e:
            print(f"Could not access full_documents table: {e}")

        # Check chunks from full content
        try:
            chunks = spark.sql(f"""
                SELECT COUNT(*) as chunk_count
                FROM {namespace}.full_document_chunks
            """).collect()

            if chunks:
                chunk_count = chunks[0]['chunk_count']
                print(f"\\nFull content chunks created: {chunk_count}")

                # Show chunk statistics
                chunk_stats = spark.sql(f"""
                    SELECT
                        chunk_strategy,
                        COUNT(*) as count,
                        AVG(chunk_word_count) as avg_words,
                        MIN(chunk_word_count) as min_words,
                        MAX(chunk_word_count) as max_words
                    FROM {namespace}.full_document_chunks
                    GROUP BY chunk_strategy
                """).collect()

                for stat in chunk_stats:
                    print(f"  Strategy {stat['chunk_strategy']}: {stat['count']} chunks")
                    print(f"    Words: {stat['min_words']}-{stat['max_words']} (avg {stat['avg_words']:.0f})")

        except Exception as e:
            print(f"Could not access chunks: {e}")

        spark.stop()

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    main()