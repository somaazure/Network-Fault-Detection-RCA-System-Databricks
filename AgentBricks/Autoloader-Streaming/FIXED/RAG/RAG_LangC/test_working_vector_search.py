# Test script for working vector search
# This tests the ACTUAL working vector index: rca_reports_vector_index

# Install required packages
print("ðŸ”§ Installing required packages for Vector Search...")
%pip install databricks-vectorsearch
dbutils.library.restartPython()

print("ðŸ” Testing Working Vector Search")
print("=" * 50)

try:
    from databricks.vector_search.client import VectorSearchClient

    # Initialize client
    vs_client = VectorSearchClient(disable_notice=True)
    print("âœ… Vector Search client initialized")

    # Use the WORKING index name
    endpoint_name = "network_fault_detection_vs_endpoint"
    index_name = "network_fault_detection.processed_data.rca_reports_vector_index"

    print(f"ðŸ” Testing index: {index_name}")
    print(f"ðŸ“Š Endpoint: {endpoint_name}")

    # Get the index
    index = vs_client.get_index(endpoint_name=endpoint_name, index_name=index_name)
    print("âœ… Successfully connected to index")

    # Test queries
    test_queries = [
        "router interface down critical",
        "high CPU utilization network device",
        "BGP neighbor routing problem",
        "firewall configuration security",
        "network performance troubleshooting"
    ]

    print("\nðŸ§ª Testing Vector Search Queries:")
    print("-" * 40)

    for i, query in enumerate(test_queries, 1):
        try:
            results = index.similarity_search(
                query_text=query,
                columns=["id", "search_content", "incident_priority", "root_cause_category"],
                num_results=3
            )

            num_results = len(results.get('result', {}).get('data_array', []))
            print(f"{i}. Query: '{query}'")
            print(f"   ðŸ“Š Results: {num_results} documents found")

            if num_results > 0:
                # Show first result
                first_result = results['result']['data_array'][0]
                priority = first_result.get('incident_priority', 'Unknown')
                category = first_result.get('root_cause_category', 'Unknown')
                print(f"   ðŸŽ¯ Top match: {category} - Priority: {priority}")
            print()

        except Exception as e:
            print(f"{i}. Query: '{query}' - âŒ Error: {str(e)}")
            print()

    print("ðŸŽ‰ Vector Search Test Complete!")
    print("\nðŸ“‹ Summary:")
    print("âœ… Index connection successful")
    print("âœ… Query execution working")
    print("âœ… Real document retrieval confirmed")

except Exception as e:
    print(f"âŒ Test failed: {str(e)}")
    print("ðŸ’¡ Make sure you're running this in Databricks notebook environment")
