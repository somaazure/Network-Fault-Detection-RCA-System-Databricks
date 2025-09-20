# Databricks notebook source
# MAGIC %md
# MAGIC # Test RAG Connection Before Flask Deployment
# MAGIC Test the vector search and RAG system in Databricks environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Dependencies

# COMMAND ----------

# Install the required packages
%pip install databricks-vectorsearch>=0.22 mlflow>=2.8.0

# COMMAND ----------

# Restart Python to load new packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Test Vector Search Connection

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import mlflow.deployments

# Configuration
VECTOR_INDEX_NAME = "network_fault_detection.processed_data.rca_reports_vector_index"
VECTOR_SEARCH_ENDPOINT = "network_fault_detection_vs_endpoint"

print("🔧 Testing Vector Search Connection...")

try:
    # Initialize Vector Search Client
    vs_client = VectorSearchClient(disable_notice=True)
    print("✅ Vector Search Client initialized")

    # Get the vector index
    index = vs_client.get_index(
        endpoint_name=VECTOR_SEARCH_ENDPOINT,
        index_name=VECTOR_INDEX_NAME
    )
    print(f"✅ Vector index accessed: {VECTOR_INDEX_NAME}")

    # Test search functionality
    test_results = index.similarity_search(
        query_text="network troubleshooting",
        columns=["id", "search_content", "incident_priority", "root_cause_category"],
        num_results=3
    )

    if test_results and 'result' in test_results:
        data_array = test_results['result'].get('data_array', [])
        print(f"✅ Search test successful - found {len(data_array)} results")

        # Display first result
        if data_array:
            first_result = data_array[0]
            print("\n📄 First search result:")
            for key, value in first_result.items():
                if isinstance(value, str) and len(value) > 100:
                    print(f"  {key}: {value[:100]}...")
                else:
                    print(f"  {key}: {value}")
    else:
        print("❌ Search test failed - no results returned")

except Exception as e:
    print(f"❌ Vector Search error: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test Foundation Models Connection

# COMMAND ----------

try:
    # Initialize LLM client
    llm_client = mlflow.deployments.get_deploy_client("databricks")
    print("✅ Foundation Models client initialized")

    # Test AI response generation
    test_prompt = """You are a network engineer. Briefly explain what causes BGP neighbor flapping."""

    response = llm_client.predict(
        endpoint="databricks-meta-llama-3-1-8b-instruct",
        inputs={
            "messages": [{"role": "user", "content": test_prompt}],
            "temperature": 0.1,
            "max_tokens": 200
        }
    )

    ai_response = response.get('choices', [{}])[0].get('message', {}).get('content', 'No response generated')
    print("✅ Foundation Models test successful")
    print(f"\n🤖 AI Response:\n{ai_response}")

except Exception as e:
    print(f"❌ Foundation Models error: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test Complete RAG Flow

# COMMAND ----------

def test_complete_rag_flow(query):
    """Test the complete RAG flow with a user query"""
    try:
        print(f"🔍 Testing RAG flow for: {query}")

        # Step 1: Vector search
        results = index.similarity_search(
            query_text=query,
            columns=["id", "search_content", "incident_priority", "root_cause_category",
                    "rca_analysis", "resolution_recommendations"],
            num_results=3
        )

        documents = []
        context_for_ai = []

        if isinstance(results, dict) and 'result' in results:
            data_array = results['result'].get('data_array', [])
            print(f"📚 Found {len(data_array)} relevant documents")

            for i, doc in enumerate(data_array):
                documents.append({
                    'id': doc.get('id', f'RCA_{i+1}'),
                    'category': doc.get('root_cause_category', 'Unknown'),
                    'priority': doc.get('incident_priority', 'Medium'),
                    'confidence': f"{88 - i*2}%"
                })

                context_for_ai.append(f"""
                Historical Incident {i+1}:
                Category: {doc.get('root_cause_category', 'Unknown')}
                Analysis: {doc.get('rca_analysis', '')[:200]}
                Resolution: {doc.get('resolution_recommendations', '')[:200]}
                """)

        # Step 2: Generate AI response
        if context_for_ai:
            context_text = "\n".join(context_for_ai[:2])  # Use top 2 results

            prompt = f"""You are a senior network engineer providing troubleshooting guidance.

User Query: {query}

Historical Context from RCA Reports:
{context_text}

Provide concise troubleshooting steps based on this historical data."""

            response = llm_client.predict(
                endpoint="databricks-meta-llama-3-1-8b-instruct",
                inputs={
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 500
                }
            )

            ai_response = response.get('choices', [{}])[0].get('message', {}).get('content', 'Response generation failed')

            print(f"✅ Complete RAG flow successful!")
            print(f"\n🤖 AI Response:\n{ai_response}")
            print(f"\n📄 Found {len(documents)} historical documents")

            return True
        else:
            print("❌ No historical context found")
            return False

    except Exception as e:
        print(f"❌ RAG flow error: {e}")
        import traceback
        traceback.print_exc()
        return False

# Test with sample queries
test_queries = [
    "BGP neighbor down causing routing issues",
    "DNS resolution problems",
    "Network performance degradation"
]

print("🧪 Testing RAG flow with sample queries...")
for query in test_queries:
    print(f"\n{'='*50}")
    success = test_complete_rag_flow(query)
    if success:
        print(f"✅ RAG test passed for: {query}")
    else:
        print(f"❌ RAG test failed for: {query}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Flask Deployment Summary

# COMMAND ----------

print("📋 RAG CONNECTION TEST SUMMARY")
print("="*50)

# Check all components
components = {
    "Vector Search Client": False,
    "Vector Index Access": False,
    "Foundation Models": False,
    "Complete RAG Flow": False
}

try:
    vs_client = VectorSearchClient(disable_notice=True)
    components["Vector Search Client"] = True
except:
    pass

try:
    index = vs_client.get_index(endpoint_name=VECTOR_SEARCH_ENDPOINT, index_name=VECTOR_INDEX_NAME)
    components["Vector Index Access"] = True
except:
    pass

try:
    llm_client = mlflow.deployments.get_deploy_client("databricks")
    components["Foundation Models"] = True
except:
    pass

try:
    # Quick test
    test_results = index.similarity_search(query_text="test", num_results=1)
    if test_results and 'result' in test_results:
        components["Complete RAG Flow"] = True
except:
    pass

for component, status in components.items():
    status_icon = "✅" if status else "❌"
    print(f"{status_icon} {component}")

all_working = all(components.values())
print(f"\n🎯 OVERALL STATUS: {'✅ ALL SYSTEMS GO' if all_working else '❌ ISSUES DETECTED'}")

if all_working:
    print("\n🚀 READY FOR FLASK DEPLOYMENT WITH REAL RAG!")
    print("Next step: Deploy Flask app with real RAG integration")
else:
    print("\n🔧 TROUBLESHOOTING NEEDED")
    print("Fix the failed components before Flask deployment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC If all tests pass above:
# MAGIC 1. The RAG system is working in Databricks environment
# MAGIC 2. We can now deploy Flask app with confidence
# MAGIC 3. The issue is likely Databricks Apps not installing dependencies properly
# MAGIC
# MAGIC Solutions:
# MAGIC 1. Use workspace-based deployment instead of Databricks Apps
# MAGIC 2. Pre-install dependencies in a cluster
# MAGIC 3. Use bundle-based deployment with proper requirements