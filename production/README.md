# Production Scripts - Databricks RAG System

Production-ready scripts for the multi-format RAG knowledge base system.

## Overview

This folder contains the production pipeline for processing documents and creating vector search indexes on Databricks. These scripts represent the best practices and optimizations learned from POC01-05.

## Current Production Version: POC05_v02 All Formats

**Features**:
- Multi-format document processing (docx, pdf, xlsx)
- Advanced extraction: tables, images, diagrams, multiple sheets
- Metadata enrichment for improved search
- Hybrid vector search (semantic + keyword)
- Bilingual support (Thai + English)

**Performance**:
- 28,823 chunks from 114 documents
- 779ms average query latency
- 100% query success rate
- 25,453 table definition chunks

## Folder Structure

```
production/
├── data_processing/
│   ├── upload_to_volume.py           # Upload documents to Databricks volume
│   ├── process_documents.py          # Multi-format document processing
│   └── add_metadata.py                # Add metadata columns for filtering
└── vector_index/
    ├── create_index.py                # Create vector search index
    ├── check_status.py                # Monitor index sync status
    └── benchmark.py                   # Performance benchmarking
```

## Quick Start

### 1. Upload Documents

```bash
cd production/data_processing
python upload_to_volume.py

# Uploads documents from local directory to:
# dbfs:/Volumes/sandbox/rdt_knowledge/rdt_document
```

### 2. Process Documents

```bash
python process_documents.py

# Processes all formats: docx, pdf, xlsx
# Output: sandbox.rdt_knowledge.poc05_v02_all_formats_chunks
# Features:
#   - Advanced table extraction
#   - Image/diagram detection
#   - Multiple sheet handling (Excel)
#   - 800 char chunks with 200 char overlap
```

### 3. Add Metadata

```bash
python add_metadata.py

# Adds metadata columns:
#   - content_category (table_definition, process_description, etc.)
#   - document_type (technical_spec, framework_guide, etc.)
#   - section_name (extracted section headings)
#   - table_name (extracted tbl_* names)
# Output: sandbox.rdt_knowledge.poc05_v02_all_formats_with_metadata
```

### 4. Create Vector Index

```bash
cd ../vector_index
python create_index.py

# Creates vector search index with:
#   - Hybrid search (semantic + keyword)
#   - Delta sync enabled
#   - Embedding model: databricks-gte-large-en
# Output: sandbox.rdt_knowledge.poc05_v02_all_formats_hybrid_rag_index
```

### 5. Monitor Index Status

```bash
python check_status.py

# Shows:
#   - Index state (ONLINE, SYNCING, etc.)
#   - Indexed row count
#   - Sync progress
```

### 6. Benchmark Performance

```bash
python benchmark.py

# Compares query performance across versions
# Tests: latency, accuracy, success rate
# Saves results to config.json
```

## Configuration

All scripts use configuration from:
```
databricks_helper/databricks_config/config.json
```

**Required settings**:
- `databricks.host`: Workspace URL
- `databricks.token`: Personal access token
- `databricks.lineage_cluster_id`: Cluster for processing
- `rag_config.vector_search_endpoint`: Vector search endpoint name
- `rag_config.embedding_model`: Embedding model endpoint

## Pipeline Flow

```
Documents (docx/pdf/xlsx)
    ↓
[upload_to_volume.py] → Databricks Volume
    ↓
[process_documents.py] → Raw Chunks (28,823)
    ↓
[add_metadata.py] → Enriched Chunks + Metadata
    ↓
[create_index.py] → Vector Index (Hybrid Search)
    ↓
[check_status.py] → Monitor Sync
    ↓
[benchmark.py] → Validate Performance
```

## Technical Details

### Document Processing (process_documents.py)

**Libraries**:
- `pdfplumber` + `PyMuPDF` for PDF extraction
- `openpyxl` + `pandas` for Excel processing
- `python-docx` for Word documents

**Features**:
- Image/diagram detection and metadata
- Multi-sheet Excel processing
- Table structure preservation
- Thai + English text handling
- Semantic chunking (800/200 chars)

### Metadata Categories (add_metadata.py)

**content_category**:
- `table_definition`: Table schemas and structures
- `table_data`: Actual table content
- `code_example`: SQL/code samples
- `process_description`: Workflow documentation
- `header`: Section headings

**document_type**:
- `technical_spec`: Technical specifications
- `framework_guide`: Framework documentation
- `process_guide`: Process descriptions
- `validation_guide`: Validation rules
- `standard_guide`: Standards and guidelines

### Vector Index (create_index.py)

**Index Configuration**:
- Type: Delta Sync Index
- Pipeline: TRIGGERED (manual sync)
- Primary Key: `id`
- Embedding Column: `content`
- Query Types: ANN (semantic), HYBRID (semantic + keyword)

**Query Examples**:

```python
# Pure semantic search
index.similarity_search(
    query_text="table structure validation",
    query_type="ANN",
    num_results=5
)

# Hybrid search (semantic + keyword)
index.similarity_search(
    query_text="tbl_ingt_job columns",
    query_type="HYBRID",
    num_results=5
)

# With metadata filter
index.similarity_search(
    query_text="validation rules",
    query_type="HYBRID",
    filters={"content_category": "table_definition"},
    num_results=5
)
```

## Performance Benchmarks

**POC05_v02 All Formats Results**:
- Knowledge Base: 28,823 chunks (17.8x vs POC04)
- Latency: 779ms average
- Top Score: 0.8177 average
- Success Rate: 100%
- Table Coverage: 25,453 table definitions

**Comparison with Previous Versions**:
| Version | Chunks | Latency | Score | Success |
|---------|--------|---------|-------|---------|
| POC03 | 1,080 | 950ms | 0.89 | 95% |
| POC04 | 1,620 | 845ms | 0.95 | 100% |
| **POC05** | **28,823** | **779ms** | **0.82** | **100%** |

## Troubleshooting

### Index Not Ready
```bash
python check_status.py
# Wait for state: ONLINE_NO_PENDING_UPDATE
# Typical sync time: 20-30 minutes for 28K chunks
```

### Query Returns No Results
- Verify index is ready (`check_status.py`)
- Check query type (ANN vs HYBRID)
- Verify columns exist in index schema
- Test with simpler query first

### Processing Errors
- Check cluster status (must be RUNNING)
- Verify file formats are supported
- Check for corrupted files
- Review error logs in output

## Maintenance

### Re-sync Index
If source table is updated:
```python
# Index auto-syncs on table changes (Delta CDF enabled)
# Or manually trigger sync via Databricks UI
```

### Update Configuration
Edit `databricks_helper/databricks_config/config.json`:
- Never commit credentials to git
- Use environment variables in production
- Keep backups of working configurations

### Monitor Performance
Run benchmark regularly:
```bash
cd vector_index
python benchmark.py
# Compare results over time
# Investigate significant changes
```

## History

This production pipeline evolved through 5 POC iterations:
- **POC01**: Initial RAG implementation
- **POC02**: Translation optimization
- **POC03**: Multi-format support (basic)
- **POC04**: Docling extraction + bilingual
- **POC05**: Hybrid search + metadata + advanced multi-format

See `archive/` folder for historical POC scripts and `docs/POC_HISTORY.md` for detailed evolution.

## Support

For issues or questions:
1. Check `docs/` folder for detailed documentation
2. Review archived POC scripts for context
3. Consult commit history for implementation details
