# Manual RAG Implementation Guide

This guide provides step-by-step manual procedures to implement the RAG Knowledge Base system while the Databricks cluster is starting or when you need to perform manual configuration.

## 🎯 Manual Implementation Overview

Since the cluster startup can take 5-10 minutes, here's what you can do manually to prepare and implement the RAG system:

---

## Phase 1: Manual Environment Preparation

### Step 1.1: Verify Databricks Workspace Setup
**Manual Actions:**
1. **Login to Databricks Workspace**
   - Navigate to: `https://adb-3001476221513652.12.azuredatabricks.net/`
   - Verify access with your credentials
   - Check Unity Catalog is enabled (look for "Data" tab in left navigation)

2. **Verify Cluster Configuration**
   - Go to Compute → Clusters
   - Find cluster: `0430-060429-u8lurdwp`
   - Verify cluster is running or can auto-start
   - Check cluster configuration:
     - Runtime: Should be 14.3 LTS or compatible
     - Node type: Standard_D4ds_v5 or similar
     - Autoscaling enabled

3. **Check Serverless Compute**
   - Go to SQL → Warehouses
   - Verify at least one warehouse is available
   - Note the warehouse ID for configuration

### Step 1.2: Manual Catalog Structure Creation
**Manual Actions via Databricks SQL Editor:**

```sql
-- Step 1: Create main catalog
CREATE CATALOG IF NOT EXISTS rag_knowledge_base;

-- Step 2: Create schemas
CREATE SCHEMA IF NOT EXISTS rag_knowledge_base.documents;
CREATE SCHEMA IF NOT EXISTS rag_knowledge_base.vectors;
CREATE SCHEMA IF NOT EXISTS rag_knowledge_base.monitoring;

-- Step 3: Verify structure
SHOW CATALOGS;
SHOW SCHEMAS IN rag_knowledge_base;
```

---

## Phase 2: Manual Data Preparation

### Step 2.1: Document Upload (Manual Process)
**Manual Actions:**

1. **Prepare Sample Documents**
   - Collect 10-20 PDF/text files for testing
   - Recommended types:
     - Company policies
     - Product documentation
     - FAQ documents
     - Technical manuals

2. **Upload via Databricks UI**
   - Navigate to Data → Create Table
   - Or use Workspace → Upload files to `/FileStore/shared_uploads/rag_documents/`
   - Upload your sample documents

3. **Create File Inventory**
   ```sql
   -- Manual inventory of uploaded files
   %fs ls /FileStore/shared_uploads/rag_documents/
   ```

### Step 2.2: Manual Delta Table Creation
**Execute via SQL Editor:**

```sql
-- Raw documents table
CREATE TABLE IF NOT EXISTS rag_knowledge_base.documents.raw_documents (
    id STRING,
    filename STRING,
    content TEXT,
    file_type STRING,
    upload_timestamp TIMESTAMP DEFAULT current_timestamp(),
    file_size_bytes BIGINT,
    file_path STRING,
    metadata MAP<STRING, STRING>
)
USING DELTA
LOCATION '/mnt/rag_data/raw_documents'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Document chunks table
CREATE TABLE IF NOT EXISTS rag_knowledge_base.documents.document_chunks (
    chunk_id STRING,
    document_id STRING,
    chunk_text TEXT,
    chunk_index INT,
    chunk_size INT,
    overlap_size INT,
    created_timestamp TIMESTAMP DEFAULT current_timestamp(),
    metadata MAP<STRING, STRING>
)
USING DELTA
LOCATION '/mnt/rag_data/document_chunks'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Verify tables created
DESCRIBE rag_knowledge_base.documents.raw_documents;
DESCRIBE rag_knowledge_base.documents.document_chunks;
```

---

## Phase 3: Manual Vector Search Setup

### Step 3.1: Vector Search Endpoint Creation (Manual via UI)
**Manual Actions:**

1. **Navigate to Machine Learning → Vector Search**
2. **Create New Endpoint:**
   - Name: `rag-knowledge-base-endpoint`
   - Type: `Standard` (for POC) or `Storage-optimized` (for production)
   - Click "Create"
   - Wait 5-10 minutes for provisioning

3. **Verify Endpoint Status:**
   - Status should show "Online"
   - Note the endpoint name for configuration

### Step 3.2: Manual Index Configuration Preparation
**Prepare Index Settings:**

```python
# Index configuration (to be used when cluster is ready)
INDEX_CONFIG = {
    "endpoint_name": "rag-knowledge-base-endpoint",
    "index_name": "rag_knowledge_base.documents.chunk_index",
    "source_table": "rag_knowledge_base.documents.document_chunks",
    "pipeline_type": "TRIGGERED",
    "primary_key": "chunk_id",
    "embedding_source_column": "chunk_text",
    "embedding_model": "databricks-gte-large-en"  # Required for Playground
}
```

---

## Phase 4: Manual Sample Data Population

### Step 4.1: Create Sample Data (Manual via SQL)
**Execute via SQL Editor:**

```sql
-- Insert sample documents for testing
INSERT INTO rag_knowledge_base.documents.raw_documents
VALUES
(
    'doc_001',
    'company_policy.txt',
    'Company Policy: Remote work is allowed 3 days per week. Employees must maintain core hours 9-3 PM local time. All meetings should be scheduled with timezone considerations.',
    'txt',
    current_timestamp(),
    150,
    '/FileStore/shared_uploads/rag_documents/company_policy.txt',
    map('source', 'manual_insert', 'category', 'policy')
),
(
    'doc_002',
    'product_faq.txt',
    'Product FAQ: Q: How do I reset my password? A: Go to Settings > Security > Reset Password. Q: What is the refund policy? A: 30-day money back guarantee for all products.',
    'txt',
    current_timestamp(),
    120,
    '/FileStore/shared_uploads/rag_documents/product_faq.txt',
    map('source', 'manual_insert', 'category', 'faq')
),
(
    'doc_003',
    'technical_guide.txt',
    'Technical Guide: To configure the system, first install dependencies: pip install requirements.txt. Then set environment variables: DATABRICKS_HOST and DATABRICKS_TOKEN.',
    'txt',
    current_timestamp(),
    180,
    '/FileStore/shared_uploads/rag_documents/technical_guide.txt',
    map('source', 'manual_insert', 'category', 'technical')
);

-- Verify data inserted
SELECT * FROM rag_knowledge_base.documents.raw_documents;
```

### Step 4.2: Manual Text Chunking
**Create Sample Chunks:**

```sql
-- Manual chunking of sample documents
INSERT INTO rag_knowledge_base.documents.document_chunks VALUES
-- Document 1 chunks
('chunk_001_001', 'doc_001', 'Company Policy: Remote work is allowed 3 days per week.', 1, 65, 0, current_timestamp(), map('document_title', 'company_policy.txt', 'section', 'remote_work')),
('chunk_001_002', 'doc_001', 'Employees must maintain core hours 9-3 PM local time. All meetings should be scheduled with timezone considerations.', 2, 108, 20, current_timestamp(), map('document_title', 'company_policy.txt', 'section', 'core_hours')),

-- Document 2 chunks
('chunk_002_001', 'doc_002', 'Product FAQ: Q: How do I reset my password? A: Go to Settings > Security > Reset Password.', 1, 95, 0, current_timestamp(), map('document_title', 'product_faq.txt', 'question_type', 'password')),
('chunk_002_002', 'doc_002', 'Q: What is the refund policy? A: 30-day money back guarantee for all products.', 2, 83, 15, current_timestamp(), map('document_title', 'product_faq.txt', 'question_type', 'refund')),

-- Document 3 chunks
('chunk_003_001', 'doc_003', 'Technical Guide: To configure the system, first install dependencies: pip install requirements.txt.', 1, 98, 0, current_timestamp(), map('document_title', 'technical_guide.txt', 'section', 'installation')),
('chunk_003_002', 'doc_003', 'Then set environment variables: DATABRICKS_HOST and DATABRICKS_TOKEN.', 2, 69, 10, current_timestamp(), map('document_title', 'technical_guide.txt', 'section', 'configuration'));

-- Verify chunks created
SELECT
    chunk_id,
    document_id,
    LEFT(chunk_text, 50) as preview,
    chunk_size,
    metadata
FROM rag_knowledge_base.documents.document_chunks
ORDER BY document_id, chunk_index;
```

---

## Phase 5: Manual AI Playground Configuration

### Step 5.1: Prepare for Agent Bricks Setup
**Manual Checklist:**

1. **Verify Prerequisites:**
   - [ ] Vector search endpoint is online
   - [ ] Vector index will be created (when cluster ready)
   - [ ] Sample data is populated
   - [ ] Embedding model is `databricks-gte-large-en`

2. **Prepare Agent Configuration:**
   ```yaml
   Agent Name: "RAG Knowledge Base Chatbot"
   Description: "Custom document knowledge base for company policies, FAQs, and technical guides"

   Knowledge Source:
     Type: "Vector Search Index"
     Index: "rag_knowledge_base.documents.chunk_index"
     Text Column: "chunk_text"
     URI Column: "document_id"

   Model Settings:
     LLM: "databricks-dbrx-instruct"
     Temperature: 0.1
     Max Tokens: 1000
     System Prompt: "You are a helpful assistant that answers questions based on company documents. Always cite your sources."
   ```

### Step 5.2: Manual Testing Queries
**Prepare Test Questions:**

```python
# Test queries for manual validation
TEST_QUERIES = [
    "What is the company policy on remote work?",
    "How many days per week can I work remotely?",
    "How do I reset my password?",
    "What is the refund policy?",
    "How do I configure the system environment?",
    "What are the core working hours?",
    "What dependencies need to be installed?"
]

# Expected answers should reference the sample documents we inserted
```

---

## Phase 6: Manual Monitoring Setup

### Step 6.1: Create Monitoring Tables
**Execute via SQL Editor:**

```sql
-- Query logs table for monitoring
CREATE TABLE IF NOT EXISTS rag_knowledge_base.monitoring.query_logs (
    query_id STRING,
    user_id STRING,
    query_text TEXT,
    response_text TEXT,
    query_timestamp TIMESTAMP DEFAULT current_timestamp(),
    response_time_ms BIGINT,
    relevance_score DOUBLE,
    sources_used ARRAY<STRING>,
    user_feedback STRING,
    metadata MAP<STRING, STRING>
)
USING DELTA
LOCATION '/mnt/rag_data/query_logs'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

-- Performance metrics table
CREATE TABLE IF NOT EXISTS rag_knowledge_base.monitoring.performance_metrics (
    metric_date DATE,
    total_queries BIGINT,
    avg_response_time_ms DOUBLE,
    avg_relevance_score DOUBLE,
    success_rate DOUBLE,
    unique_users BIGINT,
    created_timestamp TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
LOCATION '/mnt/rag_data/performance_metrics';

-- Verify monitoring tables
SHOW TABLES IN rag_knowledge_base.monitoring;
```

---

## Manual Validation Checklist

### Phase 1 Validation ✓
- [ ] Databricks workspace accessible
- [ ] Unity Catalog enabled
- [ ] Cluster configuration verified
- [ ] Catalogs and schemas created

### Phase 2 Validation ✓
- [ ] Sample documents uploaded
- [ ] Delta tables created successfully
- [ ] Sample data inserted and verified
- [ ] Text chunks created and validated

### Phase 3 Validation (Pending Cluster)
- [ ] Vector search endpoint created
- [ ] Vector index configuration prepared
- [ ] Embedding model compatibility confirmed

### Phase 4 Validation (After Cluster Ready)
- [ ] Vector index created successfully
- [ ] Embeddings generated for sample chunks
- [ ] Search functionality tested

### Phase 5 Validation (AI Playground)
- [ ] Agent Bricks configured
- [ ] Knowledge assistant connected to vector index
- [ ] Test queries executed successfully
- [ ] Citations working properly

### Phase 6 Validation (Monitoring)
- [ ] Monitoring tables created
- [ ] Performance tracking enabled
- [ ] Query logging functional

---

## Next Steps When Cluster is Ready

1. **Execute Vector Index Creation:**
   ```python
   from databricks.vector_search.client import VectorSearchClient
   vsc = VectorSearchClient()

   # Create the vector index (this needs cluster connection)
   index = vsc.create_delta_sync_index(
       endpoint_name="rag-knowledge-base-endpoint",
       index_name="rag_knowledge_base.documents.chunk_index",
       source_table_name="rag_knowledge_base.documents.document_chunks",
       pipeline_type="TRIGGERED",
       primary_key="chunk_id",
       embedding_source_column="chunk_text",
       embedding_model_endpoint_name="databricks-gte-large-en"
   )
   ```

2. **Test Vector Search:**
   ```python
   # Test similarity search
   results = vsc.similarity_search(
       index_name="rag_knowledge_base.documents.chunk_index",
       query_text="How do I work remotely?",
       columns=["chunk_text", "document_id", "metadata"],
       num_results=3
   )
   ```

3. **Configure AI Playground:**
   - Navigate to Databricks workspace → Playground
   - Select "Agent Bricks: Knowledge Assistant"
   - Use the prepared configuration above

4. **Validate End-to-End:**
   - Test all prepared queries
   - Verify citations are working
   - Check response quality and relevance

---

## Manual Troubleshooting

### Common Issues and Manual Solutions

1. **Catalog Creation Fails:**
   - Check Unity Catalog permissions
   - Verify workspace is UC-enabled
   - Try creating via UI: Data → Create Catalog

2. **Table Creation Fails:**
   - Check location permissions
   - Verify schema exists
   - Try without LOCATION clause initially

3. **Vector Search Endpoint Issues:**
   - Check workspace permissions
   - Verify region availability
   - Try Standard endpoint type first

4. **Sample Data Issues:**
   - Verify UTF-8 encoding
   - Check for special characters
   - Validate chunk sizes (not too large)

This manual approach ensures you can make progress on RAG implementation while waiting for cluster startup or when you need to perform configuration steps manually through the Databricks UI.