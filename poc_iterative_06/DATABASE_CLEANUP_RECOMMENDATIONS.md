# Database Cleanup Recommendations - sandbox.rdt_knowledge

## Summary

**Total Tables Found**: 48 tables in sandbox.rdt_knowledge catalog

**Classification**:
- **Production (POC05)**: 8 tables - KEEP
- **Archive (POC03-04)**: 3 tables - CAN DELETE
- **Other/Experimental**: 37 tables - REVIEW & CLEANUP

## Production Tables (KEEP - POC05)

These are the current production tables from POC05_v02 All Formats:

### POC05 Phase 1 (Hybrid Search + Metadata)
1. `poc05_chunks_with_metadata` - POC05 Phase 1 chunks
2. `poc05_hybrid_rag_index` - POC05 Phase 1 index

### POC05_v02 DOCX Only
3. `poc05_v02_optimized_chunks` - DOCX processing chunks
4. `poc05_v02_chunks_with_metadata` - DOCX with metadata
5. `poc05_v02_hybrid_rag_index` - DOCX hybrid index

### POC05_v02 All Formats (CURRENT PRODUCTION)
6. `poc05_v02_all_formats_chunks` - **28,823 chunks** (docx + pdf + xlsx)
7. `poc05_v02_all_formats_with_metadata` - **With metadata enrichment**
8. `poc05_v02_all_formats_hybrid_rag_index` - **PRODUCTION INDEX**

**Recommendation**:
- ✅ **KEEP all 8 tables**
- POC05_v02 All Formats is the current production version
- POC05 Phase 1 and v02 DOCX are superseded but keep for comparison

---

## Archive Tables (CAN DELETE after backup)

### POC03
1. `poc03_multimodal_rag_index` - Superseded by POC04/05

### POC04
2. `poc04_optimized_chunks` - 1,620 chunks, superseded by POC05
3. `poc04_optimized_rag_index` - Superseded by POC05 hybrid index

**Recommendation**:
- 📦 **BACKUP then DELETE**
- These are superseded by POC05
- Export to CSV/Parquet for historical reference if needed
- Delete indexes first, then tables

---

## Experimental/Other Tables (REVIEW & CLEANUP)

### Category: Early Experiments (37 tables)

#### Document Storage Tables
- `all_documents`
- `all_documents_multimodal`
- `raw_documents`
- `documents_metadata`
- `full_documents`
- `real_documents_v2`
- `production_documents`

**Recommendation**: Review if any contain unique data, otherwise DELETE

#### Chunk Tables (Various experiments)
- `all_content_rag_index`
- `all_document_chunks`
- `all_vs_chunks`
- `bilingual_chunks`
- `datax_multimodal_chunks`
- `document_chunks`
- `full_content_rag_index`
- `full_document_chunks`
- `optimized_semantic_chunks`
- `optimized_table_chunks`
- `production_chunks`
- `rag_document_chunks`
- `real_document_chunks_v2`
- `vs_document_chunks`

**Recommendation**: DELETE (superseded by POC05 structured approach)

#### Index Tables (Old versions)
- `all_content_rag_index_v1759206287`
- `all_documents_index`
- `bilingual_translation_index_v1759213209` (POC02)
- `datax_multimodal_index`
- `production_rag_index_v1759207394`
- `rag_chunk_index`
- `semantic_focused_index_v1759209612`
- `table_focused_index_v1759209612`

**Recommendation**: DELETE (old index versions)

#### Embedding Tables
- `document_embeddings`
- `embeddings_vectors`

**Recommendation**: DELETE (embeddings managed by vector index now)

#### Genie Tables
- `genie_content_summary`
- `genie_document_catalog`

**Recommendation**: REVIEW - if Genie still used, keep; otherwise DELETE

#### Monitoring Tables
- `chat_sessions`
- `rag_performance_metrics`
- `rag_query_logs`
- `system_performance`

**Recommendation**: REVIEW - if monitoring active, keep; otherwise DELETE

---

## Vector Indexes

### Production (KEEP)
1. `poc05_hybrid_rag_index` - POC05 Phase 1
2. `poc05_v02_hybrid_rag_index` - POC05_v02 DOCX
3. `poc05_v02_all_formats_hybrid_rag_index` - **CURRENT PRODUCTION**

### Archive (DELETE)
1. `poc03_multimodal_rag_index` - POC03
2. `poc04_optimized_rag_index` - POC04

**Note**: Vector indexes are separate from tables. Use VectorSearchClient to delete:
```python
vs_client.delete_index(endpoint_name="...", index_name="...")
```

---

## Cleanup Action Plan

### Phase 1: Backup (Safety)
```sql
-- Export important archive tables
CREATE TABLE archive.poc03_multimodal_rag_index AS
SELECT * FROM sandbox.rdt_knowledge.poc03_multimodal_rag_index;

CREATE TABLE archive.poc04_optimized_chunks AS
SELECT * FROM sandbox.rdt_knowledge.poc04_optimized_chunks;
```

### Phase 2: Delete Vector Indexes
```python
# Delete archive indexes
vs_client.delete_index(
    endpoint_name="rag-knowledge-base-endpoint",
    index_name="sandbox.rdt_knowledge.poc03_multimodal_rag_index"
)

vs_client.delete_index(
    endpoint_name="rag-knowledge-base-endpoint",
    index_name="sandbox.rdt_knowledge.poc04_optimized_rag_index"
)
```

### Phase 3: Delete Archive Tables
```sql
-- POC03/04
DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc03_multimodal_rag_index;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc04_optimized_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc04_optimized_rag_index;
```

### Phase 4: Delete Experimental Tables (after review)
```sql
-- Early experiments
DROP TABLE IF EXISTS sandbox.rdt_knowledge.all_content_rag_index;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.all_document_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.all_documents;
-- ... (continue for all experimental tables)
```

### Phase 5: Verify Production Still Works
```python
# Test production index
index = vs_client.get_index(
    endpoint_name="rag-knowledge-base-endpoint",
    index_name="sandbox.rdt_knowledge.poc05_v02_all_formats_hybrid_rag_index"
)

# Run test query
results = index.similarity_search(
    query_text="validation framework",
    query_type="HYBRID",
    num_results=5
)
```

---

## Space Savings Estimate

### Current State
- **Total Tables**: 48
- **Production Tables**: 8
- **Can Delete**: 40 tables

### After Cleanup
- **Remaining**: 8 production tables
- **Reduction**: 83% fewer tables
- **Storage Savings**: Significant (exact size TBD)

---

## Safety Checklist

Before deleting any table/index:

- [ ] Backup to archive catalog
- [ ] Verify no dependencies
- [ ] Check if used by any jobs/notebooks
- [ ] Test production still works
- [ ] Document what was deleted
- [ ] Keep deletion scripts for audit

---

## Recommended Cleanup Script

Create `poc_iterative_06/cleanup_database.py`:

```python
# 1. List all tables
# 2. Classify (production vs archive vs experimental)
# 3. Backup archive tables
# 4. Delete vector indexes
# 5. Delete tables
# 6. Verify production
# 7. Generate cleanup report
```

---

## Summary

**Production Tables** (8 tables):
- poc05_* tables (all variants)
- **Current**: poc05_v02_all_formats_* (3 tables)

**Archive Tables** (3 tables):
- poc03_*, poc04_*
- **Action**: Backup then delete

**Experimental Tables** (37 tables):
- Various early experiments
- **Action**: Review then delete

**Total Cleanup**: 40 out of 48 tables (83% reduction)

---

Last Updated: 2025-10-01
Status: Recommendations ready for implementation
Next: Create cleanup script and execute with approval
