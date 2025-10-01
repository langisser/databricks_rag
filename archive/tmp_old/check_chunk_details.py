#!/usr/bin/env python3
"""
Check exact chunk count per document
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json

def main():
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

        print("CHUNK COUNT PER DOCUMENT:")
        print("=" * 50)

        # Get chunk count by document
        chunk_stats = spark.sql(f"""
            SELECT
                source_file,
                COUNT(*) as chunk_count,
                AVG(chunk_word_count) as avg_words_per_chunk,
                MIN(chunk_word_count) as min_words,
                MAX(chunk_word_count) as max_words,
                SUM(chunk_word_count) as total_words,
                chunk_strategy
            FROM {namespace}.vs_document_chunks
            GROUP BY source_file, chunk_strategy
            ORDER BY source_file
        """).collect()

        for stat in chunk_stats:
            print(f"Document: {stat['source_file']}")
            print(f"  Chunks created: {stat['chunk_count']}")
            print(f"  Strategy used: {stat['chunk_strategy']}")
            print(f"  Words per chunk: {stat['min_words']} - {stat['max_words']} (avg: {stat['avg_words_per_chunk']:.0f})")
            print(f"  Total words in chunks: {stat['total_words']}")
            print()

        # Show individual chunks
        print("INDIVIDUAL CHUNKS:")
        print("=" * 50)
        chunks = spark.sql(f"""
            SELECT
                chunk_id,
                source_file,
                chunk_index,
                chunk_word_count,
                LEFT(chunk_text, 150) as preview
            FROM {namespace}.vs_document_chunks
            ORDER BY source_file, chunk_index
        """).collect()

        for chunk in chunks:
            print(f"Chunk {chunk['chunk_index'] + 1}: {chunk['chunk_id']}")
            print(f"  File: {chunk['source_file']}")
            print(f"  Words: {chunk['chunk_word_count']}")
            print(f"  Preview: {chunk['preview']}...")
            print()

        # Summary
        total_chunks = len(chunks)
        print(f"SUMMARY:")
        print(f"  Total documents processed: {len(chunk_stats)}")
        print(f"  Total chunks created: {total_chunks}")
        print(f"  Average chunks per document: {total_chunks / len(chunk_stats):.1f}")

        spark.stop()
        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    main()