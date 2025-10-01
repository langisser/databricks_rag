#!/usr/bin/env python3
"""Analyze Phase 1 hybrid search results"""
import json

with open('poc_iterative_05/phase1_ann_vs_hybrid.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print("="*80)
print("PHASE 1 ANALYSIS: HYBRID SEARCH RESULTS")
print("="*80)

print("\nOVERALL: +25% improvement with hybrid search")
print(f"  Recall: 32% -> 40% (+25%)")
print(f"  Precision: 53% -> 67% (+25%)")

print("\n" + "="*80)
print("QUERY-BY-QUERY DEEP DIVE")
print("="*80)

for i, q in enumerate(data['query_results'], 1):
    print(f"\n[{i}] {q['query']}")
    print(f"    ANN score: {q['ann']['top_score']:.6f}")
    print(f"    HYBRID score: {q['hybrid']['top_score']:.6f}")
    print(f"    ANN recall: {q['ann']['recall']:.1%}")
    print(f"    HYBRID recall: {q['hybrid']['recall']:.1%}")

    if q.get('critical'):
        print(f"    >>> CRITICAL QUERY: Still FAILED (0% recall)")

print("\n" + "="*80)
print("KEY INSIGHTS")
print("="*80)

print("""
1. HYBRID SEARCH SCORES ARE VERY HIGH (0.93 - 1.00)
   - Query 1: 0.006 -> 1.00 (167x increase!)
   - Query 2: 0.004 -> 1.00 (222x increase!)
   - Query 3: 0.003 -> 0.93 (310x increase!)

   BUT recall is still low/zero. Why?

2. HIGH SCORE ≠ RELEVANT RESULTS
   Query 3 has 0.93 score but 0% recall
   This means: Hybrid search found matches, but WRONG matches

   The keywords (LOG_ID, SUBMISSION_ID, RESPONSE_STATUS) matched,
   but not in the RIGHT CONTEXT (column definitions)

3. QUERY 2 SUCCESS (+199% recall improvement!)
   "tbl_bot_rspn_sbmt_log columns structure" - ENGLISH
   0.20 -> 0.60 recall (3x better!)

   Why did this work but Query 3 failed?

4. QUERY 3 FAILURE PATTERN
   Query: "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร"

   Problem: These column names appear EVERYWHERE in the docs:
   - In table definitions
   - In code examples
   - In process descriptions
   - In validation rules

   Hybrid search finds ALL occurrences, not just the DEFINITION

HYPOTHESIS:
The problem is NOT the search method. The problem is:
- Ground truth expects SPECIFIC chunks (column definitions)
- But column names appear in MANY chunks (dilution)
- Need METADATA FILTERING to narrow search space

SOLUTION:
Add content_type metadata:
- "table_definition" - actual table/column definitions
- "code_example" - code snippets
- "process_description" - workflow descriptions

Then query with filter:
  filters={"content_type": "table_definition"}

This will reduce false positives.
""")

print("\n" + "="*80)
print("NEXT STEPS FOR PHASE 2")
print("="*80)

print("""
PRIORITY 1: Add metadata filtering
- Add content_type column to chunks
- Tag chunks as: table_definition, code, process, etc.
- Test Query 3 with filter: content_type="table_definition"

PRIORITY 2: Improve ground truth evaluation
- Current ground truth may be too strict
- Query 3 might be finding CORRECT info but in different chunks
- Manually inspect Query 3 hybrid results to verify

PRIORITY 3: Test embedding models (lower priority)
- Hybrid search already working (high scores)
- Embedding quality less critical now
""")
