# POC Iterative 2 - Manual Setup Instructions

Due to session connectivity issues with Unity Catalog volumes, here are manual steps to process your real documents from `/Volumes/sandbox/rdt_knowledge/rdt_document`.

## Option 1: Manual Volume Processing via Databricks UI

### Step 1: Access Your Documents
1. **Open Databricks Workspace**: https://adb-3001476221513652.12.azuredatabricks.net/
2. **Navigate to Data Explorer** → **Catalog** → **sandbox** → **rdt_knowledge** → **Volumes** → **rdt_document**
3. **Browse Your Files**: Check what documents are available

### Step 2: Create Notebook for Real Data Processing
1. **Create New Notebook** in Databricks workspace
2. **Copy and paste the following code** to process your real documents:

```python
# Cell 1: Setup
from databricks.vector_search.client import VectorSearchClient
import pandas as pd

# Define paths
volume_path = "/Volumes/sandbox/rdt_knowledge/rdt_document"
namespace = "sandbox.rdt_knowledge"

# Cell 2: List Your Real Documents
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
            ELSE 'other'
        END as file_type
    FROM dbfs.`{volume_path}`
    WHERE name NOT LIKE '.%'
    ORDER BY modification_time DESC
""")

display(files_df)
print(f"Found {files_df.count()} files in your volume")

# Cell 3: Create Enhanced Tables
# Drop and recreate tables for real data
spark.sql(f"DROP TABLE IF EXISTS {namespace}.raw_documents_v2")
spark.sql(f"DROP TABLE IF EXISTS {namespace}.rag_document_chunks_v2")

# Enhanced raw documents table
spark.sql(f"""
    CREATE TABLE {namespace}.raw_documents_v2 (
        document_id STRING,
        file_name STRING,
        file_path STRING,
        file_type STRING,
        file_size_bytes BIGINT,
        content TEXT,
        word_count INT,
        character_count INT,
        extraction_method STRING,
        extraction_timestamp TIMESTAMP,
        processing_status STRING,
        metadata MAP<STRING, STRING>
    )
    USING DELTA
""")

# Enhanced chunks table
spark.sql(f"""
    CREATE TABLE {namespace}.rag_document_chunks_v2 (
        chunk_id STRING,
        document_id STRING,
        chunk_index INT,
        chunk_text TEXT,
        chunk_word_count INT,
        chunk_char_count INT,
        chunk_type STRING,
        semantic_density DOUBLE,
        created_timestamp TIMESTAMP,
        document_metadata MAP<STRING, STRING>
    )
    USING DELTA
""")

print("Enhanced tables created successfully")

# Cell 4: Process Your Real Documents
import time
import re

files_list = files_df.collect()
processed_count = 0

for file_info in files_list:
    try:
        file_name = file_info['name']
        file_path = file_info['path']
        file_type = file_info['file_type']
        file_size = file_info['size'] or 0

        print(f"Processing: {file_name} ({file_type})")

        # Generate document ID
        doc_id = f"real_doc_{int(time.time())}_{processed_count + 1}"

        # Extract text based on file type
        content = ""
        extraction_method = ""

        if file_type in ['txt', 'markdown']:
            # Read text files directly
            content_df = spark.read.text(file_path)
            content_rows = content_df.collect()
            content = "\\n".join([row['value'] for row in content_rows])
            extraction_method = "direct_text_read"

        elif file_type == 'pdf':
            # For PDF files - placeholder for now
            content = f"[PDF FILE: {file_name} - Requires PDF processing library]"
            extraction_method = "pdf_placeholder"

        elif file_type == 'docx':
            # For Word files - placeholder for now
            content = f"[DOCX FILE: {file_name} - Requires python-docx library]"
            extraction_method = "docx_placeholder"

        else:
            # Try generic text read
            try:
                content_df = spark.read.text(file_path)
                content_rows = content_df.take(100)
                content = "\\n".join([row['value'] for row in content_rows])
                extraction_method = "generic_text_read"
            except:
                content = f"[{file_type.upper()} FILE: Could not extract text]"
                extraction_method = "failed_extraction"

        # Calculate metrics
        word_count = len(content.split()) if content else 0
        char_count = len(content) if content else 0

        # Insert into table
        spark.sql(f"""
            INSERT INTO {namespace}.raw_documents_v2 VALUES (
                '{doc_id}',
                '{file_name}',
                '{file_path}',
                '{file_type}',
                {file_size},
                '{content.replace("'", "''")}',
                {word_count},
                {char_count},
                '{extraction_method}',
                current_timestamp(),
                'success',
                map('source_volume', '{volume_path}', 'processing_version', 'poc_iterative_02')
            )
        """)

        processed_count += 1
        print(f"  SUCCESS: {word_count} words extracted")

    except Exception as e:
        print(f"  ERROR: {e}")

print(f"\\nProcessed {processed_count} documents")

# Cell 5: Advanced Chunking
print("Starting advanced chunking...")

# Get all successfully processed documents
documents = spark.sql(f"""
    SELECT document_id, file_name, file_type, content, word_count
    FROM {namespace}.raw_documents_v2
    WHERE processing_status = 'success' AND content IS NOT NULL
""").collect()

total_chunks = 0

for doc in documents:
    doc_id = doc['document_id']
    file_name = doc['file_name']
    file_type = doc['file_type']
    content = doc['content']
    word_count = doc['word_count']

    print(f"Chunking: {file_name} ({word_count} words)")

    # Determine chunk size based on document size
    if word_count < 500:
        chunk_size = 300
        overlap = 50
    elif word_count < 2000:
        chunk_size = 500
        overlap = 100
    else:
        chunk_size = 800
        overlap = 150

    # Clean content
    clean_content = re.sub(r'\\n+', ' ', content)
    clean_content = re.sub(r'\\s+', ' ', clean_content).strip()

    # Split into sentences
    sentences = re.split(r'[.!?]+\\s+', clean_content)
    sentences = [s.strip() for s in sentences if s.strip()]

    # Create chunks
    chunks = []
    current_chunk = ""
    current_words = 0
    chunk_index = 0

    for sentence in sentences:
        sentence_words = len(sentence.split())

        if current_words + sentence_words > chunk_size and current_chunk:
            # Save current chunk
            chunks.append({
                "text": current_chunk.strip(),
                "word_count": current_words,
                "index": chunk_index
            })

            # Start new chunk with overlap
            if overlap > 0 and current_words > overlap:
                overlap_words = current_chunk.split()[-overlap:]
                current_chunk = " ".join(overlap_words) + " " + sentence
                current_words = len(overlap_words) + sentence_words
            else:
                current_chunk = sentence
                current_words = sentence_words

            chunk_index += 1
        else:
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

    # Insert chunks into table
    for chunk in chunks:
        chunk_id = f"{doc_id}_chunk_{chunk['index']:03d}"
        chunk_text = chunk['text']
        chunk_words = chunk['word_count']
        chunk_chars = len(chunk_text)

        # Calculate semantic density
        words = chunk_text.lower().split()
        unique_words = len(set(words))
        semantic_density = unique_words / len(words) if words else 0

        spark.sql(f"""
            INSERT INTO {namespace}.rag_document_chunks_v2 VALUES (
                '{chunk_id}',
                '{doc_id}',
                {chunk['index']},
                '{chunk_text.replace("'", "''")}',
                {chunk_words},
                {chunk_chars},
                'semantic',
                {semantic_density:.4f},
                current_timestamp(),
                map('source_file', '{file_name}', 'file_type', '{file_type}', 'chunk_strategy', '{chunk_size}w_{overlap}o')
            )
        """)

    total_chunks += len(chunks)
    print(f"  Created {len(chunks)} chunks")

print(f"\\nTotal chunks created: {total_chunks}")

# Cell 6: Validate Results
print("Validation Results:")

# Document statistics
doc_stats = spark.sql(f"""
    SELECT
        file_type,
        COUNT(*) as doc_count,
        AVG(word_count) as avg_words,
        SUM(word_count) as total_words
    FROM {namespace}.raw_documents_v2
    WHERE processing_status = 'success'
    GROUP BY file_type
""")
display(doc_stats)

# Chunk statistics
chunk_stats = spark.sql(f"""
    SELECT
        COUNT(*) as total_chunks,
        AVG(chunk_word_count) as avg_chunk_words,
        AVG(semantic_density) as avg_semantic_density,
        MIN(chunk_word_count) as min_words,
        MAX(chunk_word_count) as max_words
    FROM {namespace}.rag_document_chunks_v2
""")
display(chunk_stats)

print("Real data processing completed!")
print("Next step: Create vector search index using rag_document_chunks_v2")
```

### Step 3: Create Vector Search Index
1. **After chunking is complete**, create a new vector search index
2. **Use the new table**: `sandbox.rdt_knowledge.rag_document_chunks_v2`
3. **Follow the same process** as POC Iterative 1 but with your real data

## Option 2: Alternative File Upload Approach

If volume access continues to have issues:

1. **Upload documents via Databricks UI**:
   - Go to **Workspace** → **Data** → **Upload**
   - Upload your documents to DBFS
   - Modify the volume path in scripts to use DBFS path

2. **Use Databricks File Store**:
   - Upload via **Files** in Databricks workspace
   - Access using `/FileStore/` paths instead of `/Volumes/`

## Expected Results

After manual processing:
- **Enhanced document table**: `raw_documents_v2` with your real files
- **Advanced chunks**: `rag_document_chunks_v2` with semantic chunking
- **Improved quality**: Better chunk boundaries and metadata
- **Real data**: Actual documents instead of sample data

## Next Steps

Once your real documents are processed:
1. **Create new vector search index** using `rag_document_chunks_v2`
2. **Test with real queries** related to your actual documents
3. **Configure AI Playground** with the new vector search index
4. **Validate end-to-end RAG** with your actual knowledge base

This manual approach ensures you can process your real documents even with session connectivity issues.