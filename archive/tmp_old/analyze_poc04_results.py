#!/usr/bin/env python3
"""Deep analysis of POC04 results"""
import json

with open('poc_iterative_04/poc04_vs_poc03_comparison.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print("="*80)
print("CRITICAL ANALYSIS: WHY POC04 PERFORMED WORSE")
print("="*80)

summary = data['summary']
print(f"\nOVERALL METRICS:")
print(f"  POC03: Recall={summary['poc03_avg_recall']:.2f}, Precision={summary['poc03_avg_precision']:.2f}")
print(f"  POC04: Recall={summary['poc04_avg_recall']:.2f}, Precision={summary['poc04_avg_precision']:.2f}")
print(f"  CHANGE: {summary['recall_improvement_pct']:.1f}% recall, {summary['precision_improvement_pct']:.1f}% precision")
print(f"\n  STATUS: POC04 REGRESSED - WORSE THAN POC03")

print(f"\n" + "="*80)
print("QUERY-BY-QUERY BREAKDOWN")
print("="*80)

for i, q in enumerate(data['query_results'], 1):
    print(f"\n[{i}] {q['query']}")
    print(f"    Language: {q['language']}")
    print(f"    POC03: Recall={q['poc03']['recall']:.2f}, Precision={q['poc03']['precision']:.2f}, Score={q['poc03']['top_score']:.6f}")
    print(f"    POC04: Recall={q['poc04']['recall']:.2f}, Precision={q['poc04']['precision']:.2f}, Score={q['poc04']['top_score']:.6f}")

    if q['improvement']['recall_pct'] < -50:
        print(f"    !!! MAJOR REGRESSION: {q['improvement']['recall_pct']:.0f}% recall drop")
    elif q['improvement']['recall_pct'] < 0:
        print(f"    REGRESSION: {q['improvement']['recall_pct']:.0f}% recall drop")
    elif q['improvement']['recall_pct'] == 0:
        print(f"    NO CHANGE")
    else:
        print(f"    IMPROVEMENT: {q['improvement']['recall_pct']:.0f}% recall gain")

print(f"\n" + "="*80)
print("ROOT CAUSE ANALYSIS")
print("="*80)

print("""
KEY FINDINGS:

1. CATASTROPHIC FAILURE on Query 3:
   "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร"
   POC03: 25% recall -> POC04: 0% recall (-100%)

   WHY: This is a COLUMN-SPECIFIC query asking about specific column names.
   POC04's bilingual enrichment may have DILUTED the exact column name matches.

2. NO IMPROVEMENT on Queries 1, 2, 4, 5:
   Same recall/precision despite 20x more data

   WHY: More data != better retrieval if signal is diluted

3. SIMILARITY SCORES DROPPED:
   POC03 scores: 0.0087, 0.0075, 0.0027, 0.0030, 0.0038
   POC04 scores: 0.0063, 0.0045, 0.0029, 0.0033, 0.0053

   Generally lower, meaning semantic match quality decreased

4. DATA DILUTION EFFECT:
   POC03: 72 chunks (focused, from 1 key document)
   POC04: 1492 chunks (20x more, from 15 documents)

   Signal-to-noise ratio DECREASED

CRITICAL INSIGHT:
POC04's approach was WRONG. The problem is NOT chunking strategy or bilingual
keywords. The problem is:

A. SEMANTIC SEARCH FAILS FOR EXACT MATCHES
   Query: "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร"
   Needs: EXACT keyword matching (LOG_ID, SUBMISSION_ID, RESPONSE_STATUS)
   Got: Semantic embeddings that miss exact terms

B. LOW SIMILARITY SCORES INDICATE POOR EMBEDDING
   All scores < 0.01 (very low semantic similarity)
   Embedding model (databricks-gte-large-en) may not handle Thai well

C. BILINGUAL ENRICHMENT BACKFIRED
   Adding keywords diluted the actual content
   Made chunks less focused, harder to match
""")

print(f"\n" + "="*80)
print("POC05 RECOMMENDATIONS")
print("="*80)

print("""
PRIORITY 1: HYBRID SEARCH (Dense + Sparse)
- Keep semantic embeddings for conceptual queries
- ADD BM25 keyword search for exact term matching
- Combine scores: 0.7*semantic + 0.3*keyword

PRIORITY 2: FIX EMBEDDING MODEL
- Current: databricks-gte-large-en (English-focused, Thai is weak)
- Test: BGE-M3 or mE5-large (true multilingual)
- Similarity scores should be > 0.1 for relevant matches

PRIORITY 3: FOCUSED INDEXING
- Don't mix all 15 documents if query is about specific doc
- Add metadata filters (document_type, section, topic)
- Let user filter by document/table before semantic search

PRIORITY 4: QUERY UNDERSTANDING
- Detect query type: exact_match vs semantic vs mixed
- Route to appropriate search strategy
- "LOG_ID คืออะไร" -> keyword search first
- "อธิบาย process" -> semantic search

DO NOT:
- Add more chunking optimizations (not the problem)
- Add more metadata enrichment (makes it worse)
- Process more documents (dilution effect)
""")
