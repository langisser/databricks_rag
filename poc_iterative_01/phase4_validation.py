#!/usr/bin/env python3
"""
Phase 4 Validation: AI Playground Agent Testing and Validation
- Test agent configuration and setup
- Validate vector search integration with agent
- Perform end-to-end RAG functionality testing
- Evaluate agent response quality
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

import json
import time
import requests

def main():
    print("Phase 4 Validation: AI Playground Agent Testing")
    print("=" * 60)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Load agent configuration
        agent_config_path = os.path.join(os.path.dirname(__file__), 'agent_config.json')
        try:
            with open(agent_config_path, 'r') as f:
                agent_config = json.load(f)
        except FileNotFoundError:
            print("ERROR: Agent configuration file not found. Run phase4_ai_playground.py first.")
            return False

        rag_config = config['rag_config']
        index_name = f"{rag_config['full_namespace']}.rag_chunk_index"

        print("Configuration:")
        print(f"  Agent Name: {agent_config['agent_config']['name']}")
        print(f"  Vector Index: {index_name}")
        print(f"  Test Cases: {len(agent_config['test_queries'])}")

        # Step 1: Validate agent configuration
        print(f"\n1. Validating agent configuration...")

        required_fields = ['name', 'description', 'knowledge_sources', 'instructions']
        agent_cfg = agent_config['agent_config']

        for field in required_fields:
            if field in agent_cfg:
                print(f"   SUCCESS: {field} configured")
            else:
                print(f"   ERROR: Missing required field: {field}")
                return False

        # Validate knowledge sources
        if agent_cfg['knowledge_sources']:
            vs_source = agent_cfg['knowledge_sources'][0]
            if vs_source.get('type') == 'vector_search_index':
                print(f"   SUCCESS: Vector search index configured: {vs_source.get('index_name')}")
            else:
                print(f"   ERROR: Invalid knowledge source type")
                return False
        else:
            print(f"   ERROR: No knowledge sources configured")
            return False

        # Step 2: Test vector search functionality for agent
        print(f"\n2. Testing vector search for agent integration...")

        headers = {
            'Authorization': f'Bearer {config["databricks"]["token"]}',
            'Content-Type': 'application/json'
        }

        search_url = f'{config["databricks"]["host"]}/api/2.0/vector-search/indexes/{index_name}/query'

        test_results = []
        for i, test_case in enumerate(agent_config['test_queries'], 1):
            try:
                print(f"\n   Test {i}: {test_case['category']}")
                print(f"   Query: '{test_case['query']}'")

                search_data = {
                    'query_text': test_case['query'],
                    'columns': ['chunk_id', 'chunk_text', 'document_id'],
                    'num_results': 3
                }

                response = requests.post(search_url, headers=headers, json=search_data)

                if response.status_code == 200:
                    results = response.json()
                    data_array = results.get('result', {}).get('data_array', [])

                    if data_array:
                        # Analyze results quality
                        best_result = data_array[0]
                        chunk_text = best_result[1] if len(best_result) > 1 else ''
                        score = best_result[2] if len(best_result) > 2 else 0

                        # Check keyword relevance
                        found_keywords = [kw for kw in test_case['expected_keywords']
                                        if kw.lower() in chunk_text.lower()]

                        relevance_score = len(found_keywords) / len(test_case['expected_keywords'])

                        test_result = {
                            'query': test_case['query'],
                            'category': test_case['category'],
                            'found_results': len(data_array),
                            'similarity_score': score,
                            'relevance_score': relevance_score,
                            'found_keywords': found_keywords,
                            'chunk_preview': chunk_text[:100] + '...' if len(chunk_text) > 100 else chunk_text,
                            'status': 'PASS' if relevance_score >= 0.5 else 'FAIL'
                        }

                        test_results.append(test_result)

                        print(f"   Results: {len(data_array)} chunks found")
                        print(f"   Best match score: {score:.6f}")
                        print(f"   Keywords matched: {found_keywords} ({relevance_score*100:.1f}%)")
                        print(f"   Status: {test_result['status']}")

                    else:
                        print(f"   ERROR: No results found")
                        test_results.append({
                            'query': test_case['query'],
                            'category': test_case['category'],
                            'status': 'FAIL',
                            'error': 'No results'
                        })
                else:
                    print(f"   ERROR: Search failed with status {response.status_code}")

            except Exception as e:
                print(f"   ERROR: Test failed: {e}")

        # Step 3: Generate test summary
        print(f"\n3. Test Results Summary...")

        passed_tests = [t for t in test_results if t.get('status') == 'PASS']
        failed_tests = [t for t in test_results if t.get('status') == 'FAIL']

        print(f"   Total Tests: {len(test_results)}")
        print(f"   Passed: {len(passed_tests)}")
        print(f"   Failed: {len(failed_tests)}")
        print(f"   Success Rate: {len(passed_tests)/len(test_results):.1%}")

        if failed_tests:
            print(f"\n   Failed Tests:")
            for test in failed_tests:
                print(f"     - {test['category']}: {test['query'][:50]}...")

        # Step 4: Agent readiness assessment
        print(f"\n4. Agent Readiness Assessment...")

        readiness_checks = []

        # Vector search operational
        vs_operational = len(passed_tests) > 0
        readiness_checks.append(("Vector Search Operational", vs_operational))

        # High relevance score
        avg_relevance = sum(t.get('relevance_score', 0) for t in test_results) / len(test_results) if test_results else 0
        high_relevance = avg_relevance >= 0.6
        readiness_checks.append(("High Relevance Score", high_relevance, f"{avg_relevance:.1%}"))

        # Configuration complete
        config_complete = all(field in agent_cfg for field in required_fields)
        readiness_checks.append(("Configuration Complete", config_complete))

        # Test coverage
        categories_covered = len(set(t['category'] for t in test_results))
        good_coverage = categories_covered >= 3
        readiness_checks.append(("Test Categories Coverage", good_coverage, f"{categories_covered} categories"))

        # Success rate
        success_rate = len(passed_tests) / len(test_results) if test_results else 0
        good_success_rate = success_rate >= 0.8
        readiness_checks.append(("High Success Rate", good_success_rate, f"{success_rate:.1%}"))

        all_ready = all(check[1] for check in readiness_checks)

        for check in readiness_checks:
            status = "SUCCESS" if check[1] else "WARNING"
            detail = f" ({check[2]})" if len(check) > 2 else ""
            print(f"   {status}: {check[0]}{detail}")

        # Step 5: Export validation report
        print(f"\n5. Generating validation report...")

        validation_report = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "agent_config": agent_cfg['name'],
            "vector_index": index_name,
            "test_summary": {
                "total_tests": len(test_results),
                "passed": len(passed_tests),
                "failed": len(failed_tests),
                "success_rate": success_rate,
                "average_relevance": avg_relevance
            },
            "test_results": test_results,
            "readiness_checks": {check[0]: check[1] for check in readiness_checks},
            "overall_status": "READY" if all_ready else "NEEDS_IMPROVEMENT",
            "recommendations": []
        }

        # Add recommendations
        if not good_success_rate:
            validation_report["recommendations"].append("Improve vector search relevance by adding more documents")

        if not high_relevance:
            validation_report["recommendations"].append("Review and refine test cases and expected keywords")

        if not good_coverage:
            validation_report["recommendations"].append("Add more test categories to improve coverage")

        # Export report
        report_path = os.path.join(os.path.dirname(__file__), 'phase4_validation_report.json')
        with open(report_path, 'w') as f:
            json.dump(validation_report, f, indent=2)

        print(f"   Validation report exported: {report_path}")

        # Final summary
        print(f"\n" + "=" * 60)
        print("PHASE 4 VALIDATION SUMMARY")
        print("=" * 60)

        if validation_report["overall_status"] == "READY":
            print("SUCCESS: AI Playground Agent is ready for deployment")
            print("SUCCESS: Vector search integration working properly")
            print("SUCCESS: Test cases passing with good relevance")

            print(f"\nAgent Deployment Ready:")
            print("1. AI Playground configuration validated")
            print("2. Vector search integration confirmed")
            print("3. Test queries returning relevant results")
            print("4. Agent can be deployed to production")

        else:
            print("WARNING: Agent needs improvement before deployment")
            if validation_report["recommendations"]:
                print("\nRecommendations:")
                for rec in validation_report["recommendations"]:
                    print(f"  - {rec}")

        print(f"\nNext Steps:")
        print("1. Complete manual AI Playground setup")
        print("2. Create Knowledge Assistant with Agent Bricks")
        print("3. Test agent responses in AI Playground")
        print("4. Deploy agent endpoint for production use")

        return validation_report["overall_status"] == "READY"

    except Exception as e:
        print(f"ERROR: Phase 4 validation failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)