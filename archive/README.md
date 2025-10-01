# Archive - Historical POC Implementations

This folder contains all historical POC (Proof of Concept) iterations that led to the current production system.

## Purpose

These archived scripts are preserved for:
- **Historical Reference**: Understanding evolution and decision-making
- **Learning**: Seeing what worked and what didn't
- **Troubleshooting**: Comparing implementations when issues arise
- **Audit Trail**: Complete development history

## ⚠️ Important

**DO NOT use these scripts for production!**
Use scripts in `../production/` folder instead.

## POC Evolution Timeline

### POC01: Initial Implementation (poc_01_initial/)
**Goal**: End-to-end RAG setup
**Achievement**: Basic RAG working with single format
**Chunks**: ~500
**Format**: DOCX only
**Search**: ANN (semantic only)

**Key Scripts**:
- `phase1_environment_setup.py`: Environment validation
- `phase2_data_preparation.py`: Document loading
- `phase3_vector_search.py`: Vector index creation
- `phase4_ai_playground.py`: Query interface

**Learnings**:
- Databricks Vector Search works well
- Need better chunking strategy
- Missing metadata for filtering

---

### POC02: Optimization (poc_02_optimization/)
**Goal**: Translation optimization & Genie integration
**Achievement**: Better bilingual support
**Chunks**: ~800
**Improvement**: Thai-English optimization

**Key Scripts**:
- `analyze_and_optimize_rag.py`: Performance analysis
- `create_translation_optimized_index.py`: Bilingual index
- `fix_genie_views.py`: Genie view integration

**Learnings**:
- Bilingual search requires special handling
- Genie views useful for business users
- Still need multi-format support

---

### POC03: Multi-Format (poc_03_multiformat/)
**Goal**: Support PDF and Excel
**Achievement**: Basic multi-format processing
**Chunks**: 1,080
**Formats**: DOCX + PDF + XLSX (basic)

**Key Scripts**:
- `01_examine_volume_files.py`: File inventory
- `02_process_all_documents.py`: Multi-format processing
- `03_create_vector_index.py`: Index creation

**Learnings**:
- PDF text extraction works
- Excel needs better table handling
- Missing structured metadata

---

### POC04: Advanced Extraction (poc_04_docling/)
**Goal**: Better extraction with Docling library
**Achievement**: Improved table handling
**Chunks**: 1,620
**Library**: Docling for advanced extraction

**Key Scripts**:
- `01_extract_with_docling.py`: Docling extraction
- `02_process_optimized_chunks.py`: Optimized chunking
- `03_create_poc04_vector_index.py`: Index creation
- `04_benchmark_poc04_vs_poc03.py`: Performance comparison

**Learnings**:
- Docling good for PDFs
- Need hybrid search (not just semantic)
- Excel tables need more attention
- Metadata filtering crucial

**Performance**:
- Latency: 845ms
- Score: 0.95
- Success: 100%

---

### POC05: Production-Ready (poc_05_hybrid_search/)
**Goal**: Hybrid search + metadata + comprehensive multi-format
**Achievement**: Production-ready system
**Chunks**: 28,823 (17.8x increase!)
**Formats**: DOCX + PDF + XLSX (advanced)

**Key Scripts**:
- `06_process_all_formats_v02.py`: Advanced multi-format processing
- `07_add_metadata_all_formats.py`: Metadata enrichment
- `08_create_all_formats_index.py`: Hybrid index creation
- `09_benchmark_all_formats.py`: Comprehensive benchmarking

**Features**:
- ✅ Hybrid search (ANN + BM25)
- ✅ Metadata filtering (content_category, document_type, etc.)
- ✅ Advanced Excel: Multiple sheets, proper table formatting
- ✅ Advanced PDF: Images, diagrams detection
- ✅ 25,453 table definition chunks
- ✅ Bilingual support (Thai + English)

**Performance**:
- Latency: 779ms (-7.9% vs POC04)
- Score: 0.82 (trade-off for 17.8x coverage)
- Success: 100%
- Table Coverage: 25,453 definitions

**Supersedes**: All previous POCs
**Status**: **Migrated to production/**

---

### POC06: Cleanup & Organization (../experiments/poc_iterative_06/)
**Goal**: Code organization and cleanup
**Achievement**: Clean production structure

**Changes**:
- Created `production/` folder structure
- Archived POC01-05
- Cleaned tmp folder (80+ scripts → <10)
- Comprehensive documentation

**Status**: Active (not archived yet)

---

## Key Learnings Across POCs

### What Worked ✅
1. **Databricks Vector Search**: Reliable and performant
2. **Hybrid Search**: Better than semantic-only
3. **Metadata Filtering**: Crucial for precise results
4. **Multi-format Support**: Essential for real documents
5. **Semantic Chunking**: 800/200 char chunks optimal

### What Didn't Work ❌
1. **Single Format Only**: Too limiting
2. **No Metadata**: Hard to filter results
3. **Basic Excel Parsing**: Missed table structures
4. **ANN Only**: Not good for keyword matching
5. **Small Knowledge Base**: Not representative

### Evolution Metrics

| POC | Chunks | Latency | Score | Formats | Search |
|-----|--------|---------|-------|---------|--------|
| 01 | 500 | N/A | N/A | DOCX | ANN |
| 02 | 800 | N/A | N/A | DOCX | ANN |
| 03 | 1,080 | 950ms | 0.89 | All (basic) | ANN |
| 04 | 1,620 | 845ms | 0.95 | All (better) | ANN |
| **05** | **28,823** | **779ms** | **0.82** | **All (advanced)** | **Hybrid** |

---

## Using Archived Scripts

### View Evolution
```bash
# See progression
cd archive
ls -la

# Compare implementations
diff poc_03_multiformat/02_process_all_documents.py \
     poc_05_hybrid_search/06_process_all_formats_v02.py
```

### Reference Implementation
```bash
# Check how something was done
cd archive/poc_05_hybrid_search
cat 07_add_metadata_all_formats.py
```

### Git History
```bash
# See commit for each POC
git log --oneline --grep="POC"
```

---

## Folder Contents

### poc_01_initial/
Original end-to-end implementation scripts

### poc_02_optimization/
Translation and Genie optimization scripts

### poc_03_multiformat/
Initial multi-format support (basic)

### poc_04_docling/
Docling library integration and advanced extraction

### poc_05_hybrid_search/
**Final production version** (migrated to production/)
- Complete multi-format pipeline
- Hybrid search implementation
- Metadata enrichment
- Comprehensive benchmarking

### tmp_old/
Archived temporary scripts (58 Python files + JSON results)
- Various test and validation scripts
- Benchmark result files
- Exploratory implementations
- Keep for reference but not for use

---

## Migration to Production

The best scripts from POC05 were migrated to production:

| Archived Script | Production Location |
|----------------|-------------------|
| `06_process_all_formats_v02.py` | `production/data_processing/process_documents.py` |
| `07_add_metadata_all_formats.py` | `production/data_processing/add_metadata.py` |
| `08_create_all_formats_index.py` | `production/vector_index/create_index.py` |
| `09_benchmark_all_formats.py` | `production/vector_index/benchmark.py` |

---

## For More Information

- **Production Usage**: `../production/README.md`
- **Project Overview**: `../README.md`
- **Cleanup Details**: `../experiments/poc_iterative_06/CLEANUP_PLAN.md`

---

Last Updated: 2025-10-01
Archive Status: Complete (POC01-05)
Current Production: POC05_v02 All Formats
