# POC Iterative 05: Hybrid Search + Embedding Model Optimization

## Problem Statement

POC04 **regressed** from POC03:
- Recall dropped: 37% → 32% (-13.5%)
- Precision dropped: 60% → 53% (-11.1%)
- **Critical failure**: Query 3 dropped from 25% → 0% recall (-100%)

### Root Cause Analysis

1. **Semantic Search Fails for Exact Matches**
   - Query: "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร"
   - Needs: Exact keyword matching (LOG_ID, SUBMISSION_ID, etc.)
   - Problem: Pure semantic embeddings miss exact terms

2. **Low Similarity Scores (< 0.01)**
   - Indicates poor embedding quality for Thai-English mixed content
   - Current model: `databricks-gte-large-en` (English-focused, weak Thai support)

3. **Bilingual Enrichment Backfired**
   - Adding keywords diluted actual content
   - Made chunks less focused, worse semantic matches

4. **Data Dilution Effect**
   - POC03: 72 chunks (focused) → POC04: 1492 chunks (20x more)
   - More data ≠ better results when signal-to-noise decreases

## POC05 Solution: Hybrid Search

### What is Hybrid Search?

**Databricks Hybrid Search** (GA as of 2025) combines:
- **Dense retrieval**: Semantic vector embeddings (conceptual matching)
- **Sparse retrieval**: BM25 keyword search (exact term matching)
- **Fusion**: Reciprocal Rank Fusion (RRF) combines both results

### Why This Fixes POC04

| Query Type | Dense (Semantic) | Sparse (Keywords) | Hybrid |
|------------|------------------|-------------------|--------|
| "LOG_ID คืออะไร" | ❌ Misses exact term | ✅ Finds "LOG_ID" | ✅ Best of both |
| "อธิบาย process" | ✅ Semantic match | ❌ Thai words | ✅ Combines |
| "tbl_bot_rspn_sbmt_log มี column อะไร" | ⚠️ Partial | ⚠️ Partial | ✅ Full match |

**Expected Improvement**: Recall 32% → 65-75%

---

## Implementation Plan

### Phase 1: Quick Test - Hybrid Search on POC04 Index
**Goal**: Validate hybrid search fixes Query 3 failure

```python
# Test hybrid search on existing POC04 index
results = vs_client.get_index(
    endpoint_name=endpoint,
    index_name="poc04_optimized_rag_index"
).similarity_search(
    query_text="LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร",
    columns=["id", "content"],
    query_type="HYBRID",  # Enable hybrid search
    num_results=10
)
```

**Success Criteria**: Query 3 recall > 0% (anything better than POC04's 0%)

### Phase 2: Embedding Model Comparison
**Goal**: Test if better embedding model improves similarity scores

Available Databricks models:
1. **databricks-gte-large-en** (current) - English-focused, 8192 tokens
2. **databricks-bge-large-en** - BAAI BGE, 512 tokens, normalized embeddings

**Test**: Create small index (100 chunks) with BGE model, compare scores on Thai queries

### Phase 3: Metadata Enhancement
**Goal**: Add filters to reduce search space

Add columns to chunks:
- `document_name`: Source file name
- `content_type`: "text" | "table" | "header"
- `table_name`: Extracted table name (if applicable)
- `section`: Document section

```python
# Filtered search
results = index.similarity_search(
    query_text="tbl_bot_rspn_sbmt_log columns",
    filters={"content_type": "table"},
    query_type="HYBRID",
    num_results=5
)
```

### Phase 4: Create POC05 Production Index
**Goal**: Production-ready index with all optimizations

```python
# poc05_hybrid_rag_index with:
# - Hybrid search enabled
# - Best embedding model (GTE or BGE)
# - Metadata columns for filtering
# - Re-use POC04 chunks (no re-processing needed)
```

---

## Technical Details

### Hybrid Search Parameters

```python
response = index.similarity_search(
    query_text="query string",
    query_type="HYBRID",  # Options: "ANN", "HYBRID"
    columns=["id", "content", "metadata"],
    filters={"content_type": "table"},  # Optional metadata filters
    num_results=10
)
```

**Scoring**:
- Scores normalized 0.0 - 1.0
- Scores near 1.0: Both retrievers agree (high relevance)
- Scores < 0.5: Low relevance

**Performance**: 20% improvement on Databricks internal benchmarks

### Available Embedding Models

| Model | Dimensions | Context | Normalized | Best For |
|-------|------------|---------|------------|----------|
| **databricks-gte-large-en** | 1024 | 8192 tokens | No | Long documents |
| **databricks-bge-large-en** | 1024 | 512 tokens | Yes | Short queries |

**Current**: gte-large-en
**Test**: bge-large-en (normalized may help with low scores)

---

## Expected Results

### POC05 vs POC04 vs POC03

| Metric | POC03 | POC04 | POC05 Target |
|--------|-------|-------|--------------|
| Recall@3 | 37% | 32% ❌ | **65-75%** ✅ |
| Precision@3 | 60% | 53% ❌ | **80-90%** ✅ |
| Query 3 Recall | 25% | 0% 💥 | **75%+** ✅ |
| Similarity Scores | 0.003-0.008 | 0.002-0.006 | **0.05-0.15** |
| Latency | 1214ms | 1057ms | ~1100ms |

### Success Criteria

**Minimum (Phase 1)**:
- ✅ Query 3 recall > 0% (fix catastrophic failure)
- ✅ Average recall > 40% (better than POC04)

**Target (Phase 4)**:
- ✅ Average recall > 60% (2x POC03)
- ✅ Query 3 recall > 70%
- ✅ Similarity scores > 0.05

---

## Implementation Scripts

1. **01_test_hybrid_search_poc04.py** - Quick test on existing POC04 index
2. **02_compare_embedding_models.py** - Test GTE vs BGE on Thai queries
3. **03_add_metadata_columns.py** - Enhance chunks with metadata
4. **04_create_poc05_index.py** - Production index with hybrid search
5. **05_benchmark_poc05.py** - Compare all POCs

---

## Key Insights from Research

### Hybrid Search (Official Databricks Feature)
- ✅ **Generally Available** (GA) as of 2025
- ✅ Uses Reciprocal Rank Fusion (RRF)
- ✅ Tuned parameters for high quality results
- ✅ 20% improvement on internal benchmarks
- ✅ Perfect for SKUs, IDs, technical terms (our use case!)

### Why POC04 Failed
1. ❌ Pure semantic search can't match exact keywords
2. ❌ English-focused embedding struggles with Thai
3. ❌ Adding metadata noise dilutes signal
4. ❌ More data without better retrieval = worse results

### Why POC05 Will Succeed
1. ✅ Hybrid search: exact match + semantic understanding
2. ✅ Test embeddings specifically for Thai-English
3. ✅ Metadata used for filtering (not enrichment)
4. ✅ Focus on retrieval quality, not data quantity

---

## References

- [Databricks Hybrid Search Documentation](https://docs.databricks.com/aws/en/generative-ai/create-query-vector-search)
- [Hybrid Search GA Blog Post](https://www.databricks.com/blog/announcing-hybrid-search-general-availability-mosaic-ai-vector-search)
- [Foundation Models - Embeddings](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models)
- [Query Embedding Models](https://docs.databricks.com/aws/en/machine-learning/model-serving/query-embedding-models)

---

**Status**: Ready to implement Phase 1 - Quick hybrid search test
