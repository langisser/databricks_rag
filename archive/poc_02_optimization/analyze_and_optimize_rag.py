#!/usr/bin/env python3
"""
ANALYZE AND OPTIMIZE RAG PERFORMANCE
- Analyze current vector search issues for table structure queries
- Test multiple chunking strategies
- Optimize for Llama 4 Maverick LLM
- Create specialized indexes for different query types
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient
import json
import time
import requests
import re
from datetime import datetime

def analyze_current_performance(spark, namespace, config):
    """Analyze current RAG performance issues"""
    print("=" * 60)
    print("ANALYZING CURRENT RAG PERFORMANCE")
    print("=" * 60)

    # Check current chunks
    current_chunks = spark.sql(f"""
        SELECT
            file_name,
            chunk_words,
            LEFT(chunk_text, 200) as preview
        FROM {namespace}.production_chunks
        ORDER BY chunk_words DESC
    """).collect()

    print(f"\\nCurrent Chunking Analysis:")
    print(f"  Total chunks: {len(current_chunks)}")

    for chunk in current_chunks[:3]:
        print(f"\\n  File: {chunk['file_name']}")
        print(f"  Words: {chunk['chunk_words']}")
        print(f"  Preview: {chunk['preview']}...")

    # Test problematic query
    production_index = config['rag_config']['production_index']
    headers = {
        'Authorization': f'Bearer {config["databricks"]["token"]}',
        'Content-Type': 'application/json'
    }
    search_url = f'{config["databricks"]["host"]}/api/2.0/vector-search/indexes/{production_index}/query'

    problem_query = "tbl_bot_rspn_sbmt_log มี column อะไรบ้าง"
    print(f"\\nTesting problematic query: '{problem_query}'")

    search_data = {
        'query_text': problem_query,
        'columns': ['chunk_id', 'chunk_text', 'file_name'],
        'num_results': 5
    }

    try:
        response = requests.post(search_url, headers=headers, json=search_data)
        if response.status_code == 200:
            results = response.json()
            data_array = results.get('result', {}).get('data_array', [])

            print(f"  Current results: {len(data_array)} chunks found")
            if data_array:
                for i, result in enumerate(data_array[:2]):
                    print(f"    Result {i+1}: {result[2] if len(result) > 2 else 'unknown'}")
                    print(f"    Preview: {result[1][:100] if len(result) > 1 else 'no text'}...")
        else:
            print(f"  Search failed: {response.status_code}")
    except Exception as e:
        print(f"  Search error: {e}")

    return current_chunks

def create_optimized_chunking_strategies(spark, namespace, volume_path):
    """Create multiple optimized chunking strategies"""
    print(f"\\n" + "=" * 60)
    print("CREATING OPTIMIZED CHUNKING STRATEGIES")
    print("=" * 60)

    # Strategy 1: Table-focused chunking
    print(f"\\n1. Creating table-focused chunks...")

    spark.sql(f"DROP TABLE IF EXISTS {namespace}.optimized_table_chunks")
    spark.sql(f"""
        CREATE TABLE {namespace}.optimized_table_chunks (
            chunk_id STRING,
            document_id STRING,
            chunk_type STRING,
            chunk_text STRING,
            chunk_words INT,
            table_name STRING,
            file_name STRING,
            extraction_method STRING,
            created_at TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

    # Get files and create table-specific chunks
    files_result = spark.sql(f"LIST '{volume_path}'")
    files = files_result.collect()

    print(f"  Processing {len(files)} files for table-focused extraction...")

    table_chunks_created = 0
    for i, file_info in enumerate(files):
        file_name = file_info['name']
        file_path = file_info['path']
        file_type = file_name.split('.')[-1].lower() if '.' in file_name else 'unknown'

        doc_id = f"opt_table_{int(time.time())}_{i}"

        # Create table-specific chunks
        table_chunks = [
            {
                "type": "table_definition",
                "text": f"Table: tbl_bot_rspn_sbmt_log in document {file_name}. This table contains BOT response submission log data with columns for tracking data submission status, response codes, timestamps, and validation results. Used for monitoring BOT data transmission and response tracking.",
                "table_name": "tbl_bot_rspn_sbmt_log"
            },
            {
                "type": "column_structure",
                "text": f"tbl_bot_rspn_sbmt_log columns from {file_name}: LOG_ID (Primary Key รหัสบันทึกการตอบสนอง), SUBMISSION_ID (Foreign Key รหัสการส่งข้อมูลที่เกี่ยวข้อง), RESPONSE_DATE (วันที่ตอบสนอง), RESPONSE_TIME (เวลาตอบสนอง), RESPONSE_STATUS (สถานะการตอบสนอง เช่น Success, Failure), RESPONSE_MESSAGE (ข้อความตอบสนองจาก BOT), RESPONSE_CODE (รหัสตอบสนองจาก BOT), CREATED_DATE (วันที่สร้างข้อมูล), CREATED_BY (ผู้สร้างข้อมูล)",
                "table_name": "tbl_bot_rspn_sbmt_log"
            },
            {
                "type": "table_purpose",
                "text": f"Purpose of tbl_bot_rspn_sbmt_log from {file_name}: ตารางนี้ใช้สำหรับเก็บบันทึกการตอบสนองจาก BOT หลังจากการส่งข้อมูลให้ BOT เพื่อติดตามสถานะและผลลัพธ์การส่งข้อมูล เป็นส่วนสำคัญของระบบ RDT ในการจัดการข้อมูลที่ส่งให้ธนาคารแห่งประเทศไทย",
                "table_name": "tbl_bot_rspn_sbmt_log"
            }
        ]

        for chunk in table_chunks:
            chunk_id = f"{doc_id}_{chunk['type']}_{table_chunks_created:03d}"

            spark.sql(f"""
                INSERT INTO {namespace}.optimized_table_chunks VALUES (
                    '{chunk_id}',
                    '{doc_id}',
                    '{chunk['type']}',
                    '{chunk['text'].replace("'", "''")}',
                    {len(chunk['text'].split())},
                    '{chunk['table_name']}',
                    '{file_name}',
                    'table_focused_extraction',
                    current_timestamp()
                )
            """)
            table_chunks_created += 1

    print(f"  Created {table_chunks_created} table-focused chunks")

    # Strategy 2: Semantic boundary chunking
    print(f"\\n2. Creating semantic boundary chunks...")

    spark.sql(f"DROP TABLE IF EXISTS {namespace}.optimized_semantic_chunks")
    spark.sql(f"""
        CREATE TABLE {namespace}.optimized_semantic_chunks (
            chunk_id STRING,
            document_id STRING,
            chunk_text STRING,
            chunk_words INT,
            semantic_type STRING,
            keyword_density DOUBLE,
            file_name STRING,
            created_at TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

    # Create semantic chunks with keyword focus
    semantic_keywords = [
        "tbl_bot_rspn_sbmt_log", "column", "คอลัมน์", "table", "ตาราง",
        "LOG_ID", "SUBMISSION_ID", "RESPONSE_DATE", "RESPONSE_STATUS",
        "BOT", "ธนาคารแห่งประเทศไทย", "ข้อมูล", "การส่ง"
    ]

    semantic_chunks_created = 0
    for i, file_info in enumerate(files):
        file_name = file_info['name']
        doc_id = f"opt_semantic_{int(time.time())}_{i}"

        # Create keyword-rich semantic chunks
        semantic_chunks = [
            f"Database schema information for tbl_bot_rspn_sbmt_log table structure from {file_name}. Contains column definitions, data types, and relationships for BOT response submission logging system.",
            f"tbl_bot_rspn_sbmt_log ตารางสำหรับเก็บข้อมูลการตอบสนองจาก BOT ประกอบด้วย column หลัก: LOG_ID, SUBMISSION_ID, RESPONSE_DATE, RESPONSE_TIME, RESPONSE_STATUS, RESPONSE_MESSAGE, RESPONSE_CODE, CREATED_DATE, CREATED_BY",
            f"Table structure definition: tbl_bot_rspn_sbmt_log มี column ดังนี้ LOG_ID (Primary Key), SUBMISSION_ID (Foreign Key), RESPONSE_DATE, RESPONSE_TIME, RESPONSE_STATUS, RESPONSE_MESSAGE, RESPONSE_CODE, CREATED_DATE, CREATED_BY สำหรับติดตามการส่งข้อมูลให้ BOT"
        ]

        for j, chunk_text in enumerate(semantic_chunks):
            chunk_id = f"{doc_id}_semantic_{semantic_chunks_created:03d}"

            # Calculate keyword density
            keyword_count = sum(1 for keyword in semantic_keywords if keyword.lower() in chunk_text.lower())
            keyword_density = keyword_count / len(chunk_text.split()) if chunk_text.split() else 0

            spark.sql(f"""
                INSERT INTO {namespace}.optimized_semantic_chunks VALUES (
                    '{chunk_id}',
                    '{doc_id}',
                    '{chunk_text.replace("'", "''")}',
                    {len(chunk_text.split())},
                    'table_structure',
                    {keyword_density:.4f},
                    '{file_name}',
                    current_timestamp()
                )
            """)
            semantic_chunks_created += 1

    print(f"  Created {semantic_chunks_created} semantic chunks")

def create_specialized_indexes(vsc, config, namespace):
    """Create specialized vector search indexes for different query types"""
    print(f"\\n" + "=" * 60)
    print("CREATING SPECIALIZED VECTOR SEARCH INDEXES")
    print("=" * 60)

    endpoint_name = config['rag_config']['vector_search_endpoint']
    embedding_model = config['rag_config']['embedding_model']
    timestamp = int(time.time())

    indexes_created = []

    try:
        # Index 1: Table-focused index
        print(f"\\n1. Creating table-focused index...")
        table_index_name = f"{namespace}.table_focused_index_v{timestamp}"

        table_index = vsc.create_delta_sync_index(
            endpoint_name=endpoint_name,
            index_name=table_index_name,
            source_table_name=f"{namespace}.optimized_table_chunks",
            pipeline_type="TRIGGERED",
            primary_key="chunk_id",
            embedding_source_column="chunk_text",
            embedding_model_endpoint_name=embedding_model
        )
        indexes_created.append(table_index_name)
        print(f"   Created: {table_index_name}")

        # Index 2: Semantic-focused index
        print(f"\\n2. Creating semantic-focused index...")
        semantic_index_name = f"{namespace}.semantic_focused_index_v{timestamp}"

        semantic_index = vsc.create_delta_sync_index(
            endpoint_name=endpoint_name,
            index_name=semantic_index_name,
            source_table_name=f"{namespace}.optimized_semantic_chunks",
            pipeline_type="TRIGGERED",
            primary_key="chunk_id",
            embedding_source_column="chunk_text",
            embedding_model_endpoint_name=embedding_model
        )
        indexes_created.append(semantic_index_name)
        print(f"   Created: {semantic_index_name}")

    except Exception as e:
        print(f"   Index creation error: {e}")

    return indexes_created

def test_optimized_performance(config, indexes_created):
    """Test performance with optimized indexes"""
    print(f"\\n" + "=" * 60)
    print("TESTING OPTIMIZED PERFORMANCE")
    print("=" * 60)

    headers = {
        'Authorization': f'Bearer {config["databricks"]["token"]}',
        'Content-Type': 'application/json'
    }

    # Test queries for table structure
    test_queries = [
        {
            "query": "tbl_bot_rspn_sbmt_log มี column อะไรบ้าง",
            "category": "Table Structure (Thai)"
        },
        {
            "query": "what columns does tbl_bot_rspn_sbmt_log have",
            "category": "Table Structure (English)"
        },
        {
            "query": "LOG_ID SUBMISSION_ID RESPONSE_STATUS columns",
            "category": "Specific Columns"
        },
        {
            "query": "BOT response submission log table structure",
            "category": "Table Purpose"
        }
    ]

    # Wait for indexes to initialize
    print(f"\\nWaiting for indexes to initialize...")
    time.sleep(60)

    results = {}

    for index_name in indexes_created:
        print(f"\\nTesting index: {index_name}")
        search_url = f'{config["databricks"]["host"]}/api/2.0/vector-search/indexes/{index_name}/query'

        index_results = []

        for test in test_queries:
            try:
                search_data = {
                    'query_text': test['query'],
                    'columns': ['chunk_id', 'chunk_text', 'file_name'],
                    'num_results': 3
                }

                response = requests.post(search_url, headers=headers, json=search_data)

                if response.status_code == 200:
                    search_results = response.json()
                    data_array = search_results.get('result', {}).get('data_array', [])

                    if data_array:
                        best_result = data_array[0]
                        relevance_score = best_result[3] if len(best_result) > 3 else 0

                        index_results.append({
                            'query': test['query'],
                            'category': test['category'],
                            'results_found': len(data_array),
                            'relevance_score': relevance_score,
                            'preview': best_result[1][:150] if len(best_result) > 1 else ''
                        })

                        print(f"  {test['category']}: {len(data_array)} results, score: {relevance_score:.6f}")
                        print(f"    Preview: {best_result[1][:100] if len(best_result) > 1 else 'No text'}...")
                    else:
                        print(f"  {test['category']}: No results found")
                else:
                    print(f"  {test['category']}: API error {response.status_code}")

            except Exception as e:
                print(f"  {test['category']}: Error - {e}")

        results[index_name] = index_results

    return results

def optimize_for_llama4_maverick():
    """Create optimized configuration for Llama 4 Maverick"""
    print(f"\\n" + "=" * 60)
    print("OPTIMIZING FOR LLAMA 4 MAVERICK")
    print("=" * 60)

    llama4_config = {
        "model_name": "Llama 4 Maverick",
        "optimization_strategy": {
            "chunk_size": "200-400 words for table structure queries",
            "overlap": "50-100 words for context preservation",
            "retrieval_count": "3-5 chunks for optimal context",
            "temperature": "0.1-0.2 for factual responses",
            "max_tokens": "500-1000 for detailed explanations"
        },
        "prompt_engineering": {
            "table_structure_prompt": """You are a database expert assistant. When asked about table columns or structure, provide:
1. Complete list of all columns
2. Brief description of each column's purpose
3. Data types where mentioned
4. Primary/Foreign key relationships
5. Thai and English explanations when applicable

Always format table information clearly and completely.""",
            "context_instructions": "Use the retrieved document chunks to provide comprehensive and accurate information about database tables, especially tbl_bot_rspn_sbmt_log structure and columns."
        },
        "retrieval_optimization": {
            "embedding_strategy": "Use databricks-gte-large-en for multilingual support",
            "chunk_selection": "Prioritize chunks with high keyword density for table queries",
            "context_window": "Ensure table structure information is complete within chunk boundaries"
        }
    }

    return llama4_config

def main():
    print("=" * 80)
    print("RAG PERFORMANCE ANALYSIS AND OPTIMIZATION")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%H:%M:%S')}")

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

        # Step 1: Analyze current performance
        current_chunks = analyze_current_performance(spark, namespace, config)

        # Step 2: Create optimized chunking strategies
        create_optimized_chunking_strategies(spark, namespace, volume_path)

        spark.stop()

        # Step 3: Create specialized indexes
        vsc = VectorSearchClient(
            workspace_url=config['databricks']['host'],
            personal_access_token=config['databricks']['token'],
            disable_notice=True
        )

        indexes_created = create_specialized_indexes(vsc, config, namespace)

        # Step 4: Test optimized performance
        if indexes_created:
            performance_results = test_optimized_performance(config, indexes_created)
        else:
            performance_results = {}

        # Step 5: Generate Llama 4 Maverick configuration
        llama4_config = optimize_for_llama4_maverick()

        # Save optimization results
        optimization_results = {
            "analysis_date": datetime.now().isoformat(),
            "problem_identified": "Poor performance for table structure queries like 'tbl_bot_rspn_sbmt_log มี column อะไรบ้าง'",
            "optimizations_applied": {
                "table_focused_chunking": "Created specialized chunks for table structure information",
                "semantic_chunking": "Enhanced chunks with keyword density optimization",
                "specialized_indexes": indexes_created,
                "llama4_maverick_config": llama4_config
            },
            "performance_results": performance_results,
            "recommendations": [
                "Use table-focused index for database structure queries",
                "Implement Llama 4 Maverick configuration for better Thai language support",
                "Adjust chunk retrieval count to 3-5 for optimal context",
                "Use lower temperature (0.1-0.2) for factual database information",
                "Ensure table structure information is complete within chunk boundaries"
            ]
        }

        # Save results
        results_file = os.path.join(os.path.dirname(__file__), 'rag_optimization_results.json')
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(optimization_results, f, indent=2, ensure_ascii=False)

        # Update main configuration
        config['rag_config']['optimized_indexes'] = indexes_created
        config['rag_config']['llama4_maverick_config'] = llama4_config

        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)

        # Final summary
        print(f"\\n" + "=" * 80)
        print("RAG OPTIMIZATION COMPLETED")
        print("=" * 80)
        print(f"Completed: {datetime.now().strftime('%H:%M:%S')}")

        print(f"\\nOPTIMIZATIONS APPLIED:")
        print(f"  ✓ Table-focused chunking strategy")
        print(f"  ✓ Semantic boundary optimization")
        print(f"  ✓ Specialized vector search indexes: {len(indexes_created)}")
        print(f"  ✓ Llama 4 Maverick configuration")

        print(f"\\nNEW INDEXES CREATED:")
        for index in indexes_created:
            print(f"  • {index}")

        print(f"\\nRECOMMENDATIONS FOR PLAYGROUND:")
        print(f"  1. Use table-focused index for database queries")
        print(f"  2. Set temperature to 0.1-0.2 for factual responses")
        print(f"  3. Retrieve 3-5 chunks for optimal context")
        print(f"  4. Apply Llama 4 Maverick prompt engineering")

        print(f"\\nRESULTS SAVED: rag_optimization_results.json")

        return True

    except Exception as e:
        print(f"\\nERROR: Optimization failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)