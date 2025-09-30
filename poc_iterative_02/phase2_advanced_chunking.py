#!/usr/bin/env python3
"""
POC Iterative 2 - Phase 2: Advanced Chunking for Real Documents
- Implement semantic chunking with overlap
- Optimize chunk sizes for different document types
- Preserve document structure and hierarchy
- Enhanced metadata and relationships
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json
import time
import re

def main():
    print("=" * 60)
    print("POC ITERATIVE 2 - PHASE 2: ADVANCED CHUNKING")
    print("=" * 60)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        rag_config = config['rag_config']
        namespace = rag_config['full_namespace']

        print("1. Configuration loaded:")
        print(f"   Namespace: {namespace}")

        # Create Spark session
        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        print("   SUCCESS: Databricks Connect session established")

        # Step 2: Analyze existing documents for chunking strategy
        print(f"\n2. Analyzing documents for optimal chunking...")

        try:
            # Get document statistics
            doc_stats = spark.sql(f"""
                SELECT
                    file_type,
                    COUNT(*) as doc_count,
                    AVG(word_count) as avg_words,
                    MIN(word_count) as min_words,
                    MAX(word_count) as max_words,
                    AVG(character_count) as avg_chars
                FROM {namespace}.raw_documents_v2
                WHERE processing_status = 'success'
                GROUP BY file_type
                ORDER BY doc_count DESC
            """).collect()

            print(f"   Document analysis:")
            for stat in doc_stats:
                print(f"     {stat['file_type']}: {stat['doc_count']} docs, "
                      f"avg {stat['avg_words']:.0f} words ({stat['avg_chars']:.0f} chars)")

            # Determine optimal chunk sizes by document type
            chunk_strategies = {}
            for stat in doc_stats:
                file_type = stat['file_type']
                avg_words = stat['avg_words']

                if avg_words < 500:
                    chunk_strategies[file_type] = {"size": 300, "overlap": 50}
                elif avg_words < 2000:
                    chunk_strategies[file_type] = {"size": 500, "overlap": 100}
                else:
                    chunk_strategies[file_type] = {"size": 800, "overlap": 150}

            print(f"   Chunking strategies determined:")
            for ftype, strategy in chunk_strategies.items():
                print(f"     {ftype}: {strategy['size']} words, {strategy['overlap']} overlap")

        except Exception as e:
            print(f"   WARNING: Could not analyze documents: {e}")
            # Default strategy
            chunk_strategies = {"default": {"size": 500, "overlap": 100}}

        # Step 3: Create enhanced chunks table
        print(f"\n3. Creating enhanced document chunks table...")

        try:
            # Drop existing table if it exists
            spark.sql(f"DROP TABLE IF EXISTS {namespace}.rag_document_chunks_v2")

            # Create enhanced chunks table
            spark.sql(f"""
                CREATE TABLE {namespace}.rag_document_chunks_v2 (
                    chunk_id STRING,
                    document_id STRING,
                    chunk_index INT,
                    chunk_text TEXT,
                    chunk_word_count INT,
                    chunk_char_count INT,
                    chunk_type STRING,
                    chunk_strategy STRING,
                    overlap_start INT,
                    overlap_end INT,
                    parent_section STRING,
                    section_hierarchy ARRAY<STRING>,
                    chunk_keywords ARRAY<STRING>,
                    semantic_density DOUBLE,
                    created_timestamp TIMESTAMP,
                    document_metadata MAP<STRING, STRING>,
                    chunk_metadata MAP<STRING, STRING>
                )
                USING DELTA
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """)

            print(f"   SUCCESS: Enhanced chunks table created")

        except Exception as e:
            print(f"   ERROR: Could not create chunks table: {e}")
            return False

        # Step 4: Process documents with advanced chunking
        print(f"\n4. Processing documents with advanced chunking...")

        # Get successfully processed documents
        documents = spark.sql(f"""
            SELECT
                document_id,
                file_name,
                file_type,
                content,
                word_count,
                metadata
            FROM {namespace}.raw_documents_v2
            WHERE processing_status = 'success'
              AND content IS NOT NULL
              AND content != ''
            ORDER BY file_type, document_id
        """).collect()

        total_chunks = 0
        processed_docs = 0

        for doc in documents:
            try:
                doc_id = doc['document_id']
                file_name = doc['file_name']
                file_type = doc['file_type']
                content = doc['content']
                word_count = doc['word_count']

                print(f"   Processing: {file_name} ({word_count} words)")

                # Get chunking strategy for this file type
                strategy = chunk_strategies.get(file_type, chunk_strategies.get("default", {"size": 500, "overlap": 100}))
                chunk_size = strategy['size']
                overlap_size = strategy['overlap']

                # Advanced text preprocessing
                # Clean and normalize text
                clean_content = re.sub(r'\\n+', ' ', content)  # Replace multiple newlines with space
                clean_content = re.sub(r'\\s+', ' ', clean_content)  # Replace multiple spaces with single space
                clean_content = clean_content.strip()

                # Split into sentences for better chunking boundaries
                sentences = re.split(r'[.!?]+\\s+', clean_content)
                sentences = [s.strip() for s in sentences if s.strip()]

                # Create chunks with semantic boundaries
                chunks = []
                current_chunk = ""
                current_words = 0
                chunk_index = 0

                for sentence in sentences:
                    sentence_words = len(sentence.split())

                    # Check if adding this sentence would exceed chunk size
                    if current_words + sentence_words > chunk_size and current_chunk:
                        # Save current chunk
                        chunks.append({
                            "text": current_chunk.strip(),
                            "word_count": current_words,
                            "index": chunk_index
                        })

                        # Start new chunk with overlap
                        if overlap_size > 0 and current_words > overlap_size:
                            # Take last part of current chunk as overlap
                            overlap_words = current_chunk.split()[-overlap_size:]
                            current_chunk = " ".join(overlap_words) + " " + sentence
                            current_words = len(overlap_words) + sentence_words
                        else:
                            current_chunk = sentence
                            current_words = sentence_words

                        chunk_index += 1
                    else:
                        # Add sentence to current chunk
                        if current_chunk:
                            current_chunk += " " + sentence
                        else:
                            current_chunk = sentence
                        current_words += sentence_words

                # Add final chunk
                if current_chunk:
                    chunks.append({
                        "text": current_chunk.strip(),
                        "word_count": current_words,
                        "index": chunk_index
                    })

                print(f"     Created {len(chunks)} chunks")

                # Insert chunks into table
                for chunk in chunks:
                    chunk_id = f"{doc_id}_chunk_{chunk['index']:03d}"
                    chunk_text = chunk['text']
                    chunk_words = chunk['word_count']
                    chunk_chars = len(chunk_text)

                    # Extract keywords (simple approach)
                    words = chunk_text.lower().split()
                    keywords = list(set([w for w in words if len(w) > 4]))[:10]  # Top 10 longer words

                    # Calculate semantic density (word diversity)
                    unique_words = len(set(words))
                    semantic_density = unique_words / len(words) if words else 0

                    # Determine section hierarchy (simple approach)
                    section_hierarchy = [file_type, file_name.split('.')[0]]

                    # Chunk metadata
                    chunk_metadata = {
                        "chunk_strategy": f"{chunk_size}w_{overlap_size}o",
                        "semantic_boundary": "sentence",
                        "preprocessing": "cleaned_normalized"
                    }

                    # Insert chunk
                    spark.sql(f"""
                        INSERT INTO {namespace}.rag_document_chunks_v2 VALUES (
                            '{chunk_id}',
                            '{doc_id}',
                            {chunk['index']},
                            '{chunk_text.replace("'", "''")}',
                            {chunk_words},
                            {chunk_chars},
                            'semantic',
                            '{strategy}',
                            {overlap_size if chunk['index'] > 0 else 0},
                            {overlap_size if chunk['index'] < len(chunks) - 1 else 0},
                            '{file_type}_content',
                            array('{section_hierarchy[0]}', '{section_hierarchy[1]}'),
                            array({', '.join([f"'{kw}'" for kw in keywords[:5]])}),
                            {semantic_density:.4f},
                            current_timestamp(),
                            map(
                                'source_file', '{file_name}',
                                'file_type', '{file_type}',
                                'original_word_count', '{word_count}'
                            ),
                            map(
                                'chunk_strategy', '{chunk_metadata["chunk_strategy"]}',
                                'semantic_boundary', '{chunk_metadata["semantic_boundary"]}',
                                'preprocessing', '{chunk_metadata["preprocessing"]}'
                            )
                        )
                    """)

                total_chunks += len(chunks)
                processed_docs += 1

            except Exception as e:
                print(f"     ERROR: Failed to chunk {file_name}: {e}")

        # Step 5: Validation and quality analysis
        print(f"\n5. Chunking quality analysis...")

        # Analyze chunk quality
        quality_stats = spark.sql(f"""
            SELECT
                chunk_type,
                chunk_strategy,
                COUNT(*) as chunk_count,
                AVG(chunk_word_count) as avg_words,
                AVG(chunk_char_count) as avg_chars,
                AVG(semantic_density) as avg_semantic_density,
                MIN(chunk_word_count) as min_words,
                MAX(chunk_word_count) as max_words
            FROM {namespace}.rag_document_chunks_v2
            GROUP BY chunk_type, chunk_strategy
            ORDER BY chunk_count DESC
        """).collect()

        print(f"   Chunk Quality Analysis:")
        for stat in quality_stats:
            print(f"     Strategy {stat['chunk_strategy']}: {stat['chunk_count']} chunks")
            print(f"       Avg: {stat['avg_words']:.0f} words, density: {stat['avg_semantic_density']:.3f}")
            print(f"       Range: {stat['min_words']}-{stat['max_words']} words")

        # Check for optimal chunk distribution
        chunk_dist = spark.sql(f"""
            SELECT
                CASE
                    WHEN chunk_word_count < 200 THEN 'Too Small'
                    WHEN chunk_word_count BETWEEN 200 AND 800 THEN 'Optimal'
                    WHEN chunk_word_count > 800 THEN 'Too Large'
                END as size_category,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM {namespace}.rag_document_chunks_v2
            GROUP BY size_category
            ORDER BY count DESC
        """).collect()

        print(f"\n   Chunk Size Distribution:")
        for dist in chunk_dist:
            print(f"     {dist['size_category']}: {dist['count']} chunks ({dist['percentage']}%)")

        spark.stop()

        print(f"\n" + "=" * 60)
        print("PHASE 2 ADVANCED CHUNKING COMPLETED")
        print("=" * 60)
        print(f"STATUS: Real documents chunked with advanced strategies")
        print(f"PROCESSED: {processed_docs} documents → {total_chunks} chunks")
        print(f"TABLE: {namespace}.rag_document_chunks_v2 with enhanced metadata")
        print("NEXT: Phase 3 - Enhanced vector search optimization")

        return True

    except Exception as e:
        print(f"\nERROR: Phase 2 advanced chunking failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)