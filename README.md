# Databricks RAG Knowledge Base System

Production-ready multi-format RAG (Retrieval-Augmented Generation) system on Databricks with hybrid vector search, metadata filtering, and bilingual support.

## Quick Start

```bash
# 1. Configure
cd databricks_helper/databricks_config
# Edit config.json with your credentials

# 2. Upload documents
cd ../../production/data_processing
python upload_to_volume.py

# 3. Process & create index
python process_documents.py
python add_metadata.py
cd ../vector_index
python create_index.py

# 4. Monitor & benchmark
python check_status.py
python benchmark.py
```

## Project Structure

```
databricks_rag/
├── production/              # Production scripts (use these!)
│   ├── data_processing/    # Document processing
│   └── vector_index/       # Index management
├── utilities/              # Helper utilities
├── archive/                # Historical POCs (reference only)
├── docs/                   # Documentation
└── databricks_helper/      # Config & helpers
```

## Current Performance

**POC05_v02 All Formats**:
- 28,823 chunks from 114 documents
- 779ms average latency
- 100% success rate
- Supports: docx, pdf, xlsx

## Documentation

- **Production Guide**: `production/README.md`
- **Cleanup Plan**: `poc_iterative_06/CLEANUP_PLAN.md`
- **Architecture**: `docs/ARCHITECTURE.md` (coming soon)

## Evolution

POC01 → POC02 → POC03 → POC04 → **POC05** (current) → POC06 (cleanup)

See `archive/` for historical implementations.

---
Last Updated: 2025-10-01 | Status: Production Ready
