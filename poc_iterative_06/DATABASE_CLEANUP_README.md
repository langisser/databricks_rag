# Database Cleanup Instructions

## Overview

Keep only **POC05_v02 All Formats** (3 tables + 1 index)
Delete everything else (45 tables + old indexes)

## What to Keep

### Tables (3)
1. `poc05_v02_all_formats_chunks` - 28,823 chunks
2. `poc05_v02_all_formats_with_metadata` - Chunks with metadata
3. `poc05_v02_all_formats_hybrid_rag_index` - Index table

### Vector Index (1)
- `sandbox.rdt_knowledge.poc05_v02_all_formats_hybrid_rag_index`

## What to Delete

### Tables (45)
- POC05 Phase 1: 2 tables
- POC05_v02 DOCX: 3 tables
- POC03/04: 3 tables
- Experimental: 37 tables

### Vector Indexes (estimated 10-15)
- POC05 old variants
- POC03/04 indexes
- Experimental indexes

## Cleanup Files

### 1. cleanup_commands.sql
**SQL commands to delete tables**

Usage:
```sql
-- Open in Databricks SQL Editor
-- Execute section by section
-- Verify each section before running next
```

Sections:
- Section 1: Delete POC05 Phase 1 (2 tables)
- Section 2: Delete POC05_v02 DOCX (3 tables)
- Section 3: Delete POC03/04 (3 tables)
- Section 4-9: Delete experimental tables (37 tables)

### 2. cleanup_vector_indexes.py
**Python script to delete vector indexes**

Usage:
```bash
cd poc_iterative_06
python cleanup_vector_indexes.py

# Script will:
# 1. List all indexes to delete
# 2. Ask for confirmation for each
# 3. Delete confirmed indexes
# 4. Verify production index still exists
```

## Execution Steps

### Step 1: Backup (Optional but Recommended)
```sql
-- Create backup of important tables
CREATE TABLE archive.poc04_optimized_chunks AS
SELECT * FROM sandbox.rdt_knowledge.poc04_optimized_chunks;

-- Or export to CSV/Parquet
```

### Step 2: Delete Vector Indexes
```bash
# Run Python script
cd poc_iterative_06
python cleanup_vector_indexes.py

# Follow prompts to confirm each deletion
```

### Step 3: Delete Tables
```sql
-- Open cleanup_commands.sql in Databricks SQL Editor

-- Execute Section 1 (POC05 Phase 1)
-- Verify results

-- Execute Section 2 (POC05_v02 DOCX)
-- Verify results

-- Execute Section 3 (POC03/04)
-- Verify results

-- Execute Sections 4-9 (Experimental tables)
-- Review each section before executing
```

### Step 4: Verify
```sql
-- Should show only 3 tables
SHOW TABLES IN sandbox.rdt_knowledge;
```

Expected result:
```
poc05_v02_all_formats_chunks
poc05_v02_all_formats_with_metadata
poc05_v02_all_formats_hybrid_rag_index
```

### Step 5: Test Production
```python
from databricks.vector_search.client import VectorSearchClient

# Get production index
index = vs_client.get_index(
    endpoint_name="rag-knowledge-base-endpoint",
    index_name="sandbox.rdt_knowledge.poc05_v02_all_formats_hybrid_rag_index"
)

# Test query
results = index.similarity_search(
    query_text="validation framework guidelines",
    query_type="HYBRID",
    num_results=5
)

# Should return 5 results with good scores
```

## Safety Checklist

Before executing cleanup:

- [ ] Review all files (cleanup_commands.sql, cleanup_vector_indexes.py)
- [ ] Understand what will be deleted
- [ ] Have backup plan if needed
- [ ] Test production index still works
- [ ] Document the cleanup

During execution:

- [ ] Execute one section at a time
- [ ] Verify after each section
- [ ] Stop if any errors occur
- [ ] Monitor for dependencies

After cleanup:

- [ ] Verify only 3 tables remain
- [ ] Test production queries work
- [ ] Monitor performance
- [ ] Update documentation

## Rollback Plan

If something goes wrong:

1. **Stop immediately**
2. **Check what was deleted**
3. **Restore from backup if needed**
4. **Document the issue**

Note: Vector indexes can be recreated from tables using:
```python
vs_client.create_delta_sync_index(...)
```

## Expected Results

### Before
- Tables: 48
- Vector Indexes: ~15
- Storage: Large

### After
- Tables: 3 (POC05_v02 All Formats only)
- Vector Indexes: 1 (Production)
- Storage: Reduced by ~90%

### Benefits
- ✅ Cleaner database
- ✅ Easier to maintain
- ✅ Lower storage costs
- ✅ Clear production path
- ✅ No confusion about which tables to use

## Troubleshooting

### "Table does not exist"
- Table already deleted or never existed
- Continue with next table

### "Index does not exist"
- Index already deleted or never existed
- Continue with next index

### "Cannot drop table: dependencies exist"
- Check if table is used by views or other objects
- Drop dependencies first
- Or skip that table for now

### "Permission denied"
- Ensure you have DROP permissions on sandbox.rdt_knowledge
- Contact admin if needed

## Monitoring After Cleanup

### Check table sizes
```sql
DESCRIBE DETAIL sandbox.rdt_knowledge.poc05_v02_all_formats_with_metadata;
```

### Monitor queries
```python
# Run benchmark
cd ../production/vector_index
python benchmark.py
```

### Check logs
- Monitor for any errors
- Verify query performance unchanged
- Check if any jobs failed

## Contact

For issues or questions:
- Review DATABASE_CLEANUP_RECOMMENDATIONS.md
- Check production/README.md for production usage
- Consult archive/README.md for historical context

---

**Created**: 2025-10-01
**Status**: Ready for manual execution
**Risk**: Low (with proper verification)
**Time**: ~15-30 minutes
