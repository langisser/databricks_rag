#!/usr/bin/env python3
"""
Show loaded documents and chunks in tables
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json

def main():
    print("Showing Loaded Documents and Chunks")
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

        # 1. Show documents table
        print(f"\n1. DOCUMENTS TABLE ({namespace}.real_documents_v2):")
        print("=" * 80)

        docs_df = spark.sql(f"""
            SELECT
                document_id,
                file_name,
                file_type,
                file_size_kb,
                word_count,
                processing_status,
                LEFT(content, 150) as content_preview
            FROM {namespace}.real_documents_v2
            ORDER BY file_name
        """)

        docs = docs_df.collect()

        if docs:
            print(f"Found {len(docs)} documents loaded:")
            print()
            for i, doc in enumerate(docs, 1):
                print(f"Document {i}: {doc['file_name']}")
                print(f"  ID: {doc['document_id']}")
                print(f"  Type: {doc['file_type']}")
                print(f"  Size: {doc['file_size_kb']} KB")
                print(f"  Words: {doc['word_count']}")
                print(f"  Status: {doc['processing_status']}")
                print(f"  Content Preview: {doc['content_preview']}...")
                print()
        else:
            print("No documents found in table")

        # 2. Show full content of one document as example
        print(f"\n2. FULL CONTENT EXAMPLE:")
        print("=" * 80)

        full_content = spark.sql(f"""
            SELECT file_name, content, word_count
            FROM {namespace}.real_documents_v2
            WHERE file_name LIKE '%Counterparty%'
            LIMIT 1
        """).collect()

        if full_content:
            doc = full_content[0]
            print(f"Full content of: {doc['file_name']} ({doc['word_count']} words)")
            print("-" * 60)
            print(doc['content'])
            print("-" * 60)
        else:
            print("No Counterparty document found")

        # 3. Show chunks table
        print(f"\n3. CHUNKS TABLE ({namespace}.real_document_chunks_v2):")
        print("=" * 80)

        chunks_df = spark.sql(f"""
            SELECT
                chunk_id,
                document_id,
                chunk_index,
                chunk_word_count,
                chunk_strategy,
                semantic_density,
                LEFT(chunk_text, 100) as chunk_preview
            FROM {namespace}.real_document_chunks_v2
            ORDER BY document_id, chunk_index
        """)

        chunks = chunks_df.collect()

        if chunks:
            print(f"Found {len(chunks)} chunks loaded:")
            print()
            for chunk in chunks:
                print(f"Chunk: {chunk['chunk_id']}")
                print(f"  Document: {chunk['document_id']}")
                print(f"  Index: {chunk['chunk_index']}")
                print(f"  Words: {chunk['chunk_word_count']}")
                print(f"  Strategy: {chunk['chunk_strategy']}")
                print(f"  Density: {chunk['semantic_density']:.3f}")
                print(f"  Preview: {chunk['chunk_preview']}...")
                print()
        else:
            print("No chunks found in table")

        # 4. Show one full chunk as example
        print(f"\n4. FULL CHUNK EXAMPLE:")
        print("=" * 80)

        full_chunk = spark.sql(f"""
            SELECT chunk_id, chunk_text, chunk_word_count
            FROM {namespace}.real_document_chunks_v2
            LIMIT 1
        """).collect()

        if full_chunk:
            chunk = full_chunk[0]
            print(f"Full chunk: {chunk['chunk_id']} ({chunk['chunk_word_count']} words)")
            print("-" * 60)
            print(chunk['chunk_text'])
            print("-" * 60)
        else:
            print("No chunks found")

        # 5. Table statistics
        print(f"\n5. TABLE STATISTICS:")
        print("=" * 80)

        # Document stats
        doc_stats = spark.sql(f"""
            SELECT
                COUNT(*) as total_docs,
                SUM(word_count) as total_words,
                AVG(word_count) as avg_words_per_doc,
                MIN(word_count) as min_words,
                MAX(word_count) as max_words
            FROM {namespace}.real_documents_v2
        """).collect()[0]

        print("Document Statistics:")
        print(f"  Total documents: {doc_stats['total_docs']}")
        print(f"  Total words: {doc_stats['total_words']}")
        print(f"  Average words per document: {doc_stats['avg_words_per_doc']:.1f}")
        print(f"  Word range: {doc_stats['min_words']} - {doc_stats['max_words']}")

        # Chunk stats
        chunk_stats = spark.sql(f"""
            SELECT
                COUNT(*) as total_chunks,
                AVG(chunk_word_count) as avg_words_per_chunk,
                AVG(semantic_density) as avg_semantic_density,
                MIN(chunk_word_count) as min_chunk_words,
                MAX(chunk_word_count) as max_chunk_words
            FROM {namespace}.real_document_chunks_v2
        """).collect()[0]

        print("\nChunk Statistics:")
        print(f"  Total chunks: {chunk_stats['total_chunks']}")
        print(f"  Average words per chunk: {chunk_stats['avg_words_per_chunk']:.1f}")
        print(f"  Average semantic density: {chunk_stats['avg_semantic_density']:.3f}")
        print(f"  Chunk word range: {chunk_stats['min_chunk_words']} - {chunk_stats['max_chunk_words']}")

        spark.stop()

        print(f"\n" + "=" * 50)
        print("TABLE INSPECTION COMPLETED")
        print("=" * 50)

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    main()