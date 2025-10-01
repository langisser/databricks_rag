# POC05 Phase 1 Findings: Hybrid Search Results

## Executive Summary

**Hybrid Search Success**: +25% improvement overall (Recall: 32% → 40%, Precision: 53% → 67%)

**Critical Failure Persists**: Query 3 still has 0% recall despite hybrid search

**Root Cause Identified**: Hybrid search finds wrong chunks - mentions keywords but not definitions

---

## Test Results

### Overall Metrics: ANN vs HYBRID

| Metric | ANN (Semantic Only) | HYBRID (Semantic + Keyword) | Improvement |
|--------|---------------------|------------------------------|-------------|
| Recall@3 | 32% | 40% | **+25%** ✅ |
| Precision@3 | 53% | 67% | **+25%** ✅ |
| Avg Latency | 1361ms | 1430ms | +5% (acceptable) |

### Query-by-Query Results

| Query | Type | ANN Recall | HYBRID Recall | Change | ANN Score | HYBRID Score |
|-------|------|------------|---------------|--------|-----------|--------------|
| 1. tbl_bot_rspn_sbmt_log มี column อะไรบ้าง | Thai | 20% | 20% | 0% | 0.006 | **1.000** |
| 2. tbl_bot_rspn_sbmt_log columns structure | English | 20% | **60%** | **+199%** ✅ | 0.005 | **1.000** |
| 3. LOG_ID SUBMISSION_ID ... คือ column อะไร | Mixed | 0% | **0%** | 0% ❌ | 0.003 | **0.932** |
| 4. ตาราง BOT response log structure | Mixed | 60% | 60% | 0% | 0.003 | **0.992** |
| 5. DataX OPM RDT Submission to BOT | English | 60% | 60% | 0% | 0.005 | **0.871** |

### Key Observations

1. **Scores Dramatically Increased** (167x - 222x!)
   - ANN scores: 0.003 - 0.006 (very low semantic similarity)
   - HYBRID scores: 0.87 - 1.00 (near-perfect keyword matches)
   - This proves hybrid search IS working

2. **Query 2 Major Success** (+199% recall improvement)
   - English query: "tbl_bot_rspn_sbmt_log columns structure"
   - Recall jumped from 20% → 60%
   - Shows hybrid search CAN work for table queries

3. **Query 3 Paradox**: High score (0.93) but 0% recall
   - Hybrid search found matches
   - But found WRONG matches
   - Keywords matched, but not in correct context

---

## Deep Dive: Why Query 3 Failed

### The Query
```
"LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร"
(Translation: "What are the LOG_ID, SUBMISSION_ID, RESPONSE_STATUS columns?")
```

### Expected Keywords
- LOG_ID
- SUBMISSION_ID
- RESPONSE_STATUS
- column

### What Hybrid Search Actually Retrieved (Top 10 Results)

**Result 1 (Score: 0.932)**
- Keywords found: **0/4** ❌
- Content: Thai text about "submission data" and "data entity"
- Issue: Generic submission process description, no column definitions

**Result 2 (Score: 0.883)**
- Keywords found: **0/4** ❌
- Content: Thai text about "submission data classification"
- Issue: High-level process description

**Result 3 (Score: 0.868)**
- Keywords found: **0/4** ❌
- Content: Thai text about "submission chunks"
- Issue: Process description, not column definitions

**Results 5-10 (Scores: 0.5 - 0.6)**
- Keywords found: 1/4 (only found "column" keyword)
- Content: Various tables with different columns
- Issue: Wrong tables, not tbl_bot_rspn_sbmt_log

### Root Cause Analysis

**Problem**: Hybrid search matched on THAI words in the query, not the ENGLISH column names

The query has:
- English: `LOG_ID`, `SUBMISSION_ID`, `RESPONSE_STATUS`
- Thai: `คือ column อะไร` (what are the columns)

Hybrid search heavily weighted the Thai text and found chunks about "submission" (การส่งข้อมูล), but completely missed the specific English column names.

---

## Why Query 2 Succeeded But Query 3 Failed

### Query 2: "tbl_bot_rspn_sbmt_log columns structure" ✅
- **ALL ENGLISH**
- Keywords: table name + "columns" + "structure"
- Clear intent: find table structure
- Hybrid search found exact table name match

### Query 3: "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร" ❌
- **MIXED** Thai + English
- English: specific column names
- Thai: question phrase
- Hybrid search weighted Thai phrase too heavily, missed English column names

---

## Insights for POC05

### What We Learned

1. **Hybrid Search Works** (+25% average improvement)
   - Especially effective for pure English queries
   - Dramatically improves scores (0.006 → 1.00)
   - Solves some cases (Query 2: +199% recall)

2. **Mixed Language Queries Are Hard**
   - Hybrid search struggles with Thai question + English technical terms
   - May need different strategies for mixed queries

3. **Ground Truth May Be Too Strict**
   - Query 3 might be finding CORRECT information
   - But in different chunks than ground truth expects
   - Need to manually validate if results are actually useful

4. **Need Better Filtering**
   - Even with hybrid search, need to filter by content type
   - Distinguish "table definition" from "process description"
   - Metadata filtering is critical

### What Doesn't Work

1. ❌ Pure semantic search (too low scores)
2. ❌ Hybrid search alone (without metadata filtering)
3. ❌ Bilingual keyword enrichment in POC04 (diluted signal)

### What Does Work

1. ✅ Hybrid search for English queries (+199% for Query 2)
2. ✅ High keyword matching scores (0.87 - 1.00)
3. ✅ Overall improvement (+25% recall)

---

## Recommendations for Next Phases

### PRIORITY 1: Add Metadata Filtering (CRITICAL)

**Problem**: Hybrid search finds keywords everywhere, not just in definitions

**Solution**: Add `content_type` metadata to chunks:
- `table_definition` - actual table/column definitions
- `table_data` - table data examples
- `code_example` - code snippets
- `process_description` - workflow descriptions

**Implementation**:
```python
# Re-process POC04 chunks with content_type
spark.sql(f"""
    ALTER TABLE {poc04_table}
    ADD COLUMNS (content_type STRING, document_type STRING, section_name STRING)
""")

# Then query with filter
results = index.similarity_search(
    query_text="LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร",
    filters={"content_type": "table_definition"},
    query_type="HYBRID",
    num_results=5
)
```

**Expected Impact**: Query 3 should improve from 0% → 50-75% recall

### PRIORITY 2: Improve Mixed Language Handling

**Option A**: Query Translation
- Detect mixed queries
- Translate Thai to English before search
- "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร"
  → "LOG_ID SUBMISSION_ID RESPONSE_STATUS columns what are"

**Option B**: Query Expansion
- Keep original query
- Add English equivalent of Thai phrase
- "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร columns definition"

**Expected Impact**: +20-30% for mixed queries

### PRIORITY 3: Validate Ground Truth

**Issue**: Maybe Query 3 IS finding correct info, just in different chunks

**Action**: Manually review Query 3 hybrid results
- Are top results actually useful?
- Do they answer the question?
- Ground truth might need updating

### PRIORITY 4: Test Embedding Models (Lower Priority)

**Status**: Hybrid search is working (high scores)
- ANN scores were low (0.003-0.006) but hybrid fixed this (0.87-1.00)
- Embedding quality less critical now that we have keyword matching
- Can test later if other improvements don't suffice

---

## Phase 2 Plan

### Quick Win (2-3 hours): Add Metadata Filtering
1. Add columns: `content_type`, `document_type`, `section_name` to POC04 table
2. Re-process chunks with content type detection
3. Test Query 3 with filter: `content_type="table_definition"`
4. Measure improvement

### Medium Term (4-6 hours): Mixed Language Optimization
1. Detect mixed Thai-English queries
2. Implement query expansion strategy
3. Test on all 5 benchmark queries
4. Compare with Phase 1 results

### Full POC05 (1-2 days): Complete Implementation
1. Create poc05_hybrid_rag_index with metadata columns
2. Implement query routing (pure English vs mixed)
3. Add metadata filters for all query types
4. Full benchmark comparison: POC05 vs POC04 vs POC03
5. Documentation and findings

---

## Success Criteria for POC05

### Minimum Acceptable
- ✅ Average recall > 45% (better than POC04's 32%)
- ✅ Query 3 recall > 25% (fix the catastrophic failure)
- ✅ No regression on Query 2 (keep the +199% gain)

### Target Goals
- 🎯 Average recall > 60% (2x POC03's 37%)
- 🎯 Query 3 recall > 70%
- 🎯 All queries > 40% recall (no failures)

### Stretch Goals
- 🚀 Average recall > 70%
- 🚀 Average precision > 85%
- 🚀 Sub-1000ms latency

---

## Conclusion

**Phase 1 Status**: ✅ Successful validation - Hybrid search improves results but needs metadata filtering

**Key Finding**: High scores (0.93) don't guarantee relevant results - need content type filtering

**Next Step**: Implement metadata filtering (Priority 1) to fix Query 3 failure

**Timeline**: 2-3 hours for quick win, 1-2 days for full POC05 implementation
