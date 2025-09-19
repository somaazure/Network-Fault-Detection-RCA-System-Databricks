# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ” Basic RAG Search (Fallback - No Vector Search Required)
# MAGIC
# MAGIC **Purpose**: Intelligent search on RCA reports using text matching + AI responses
# MAGIC **Use Case**: When Vector Search is not available in workspace
# MAGIC **Features**: Keyword search, AI-powered responses, search analytics

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import mlflow
import json
from datetime import datetime
from typing import List, Dict, Any

spark = SparkSession.builder.getOrCreate()

print("ðŸ” Basic RAG Search System (Vector Search Fallback)")
print("=" * 70)

# COMMAND ----------

# Configuration
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"
RCA_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_reports_streaming"
FOUNDATION_MODEL_NAME = "databricks-meta-llama-3-1-8b-instruct"

# COMMAND ----------

class BasicRAGSearchEngine:
    """Basic intelligent search engine for network fault RCA reports"""

    def __init__(self):
        self.rca_table = RCA_TABLE
        self.foundation_model = FOUNDATION_MODEL_NAME
        self.search_history = []

        print("ðŸ”§ Initializing Basic RAG Search Engine...")
        self._validate_setup()

    def _validate_setup(self):
        """Validate that required data is available"""
        try:
            rca_df = spark.table(self.rca_table)
            count = rca_df.count()

            if count > 0:
                print(f"âœ… RCA data available: {count:,} records")
                self.data_available = True
            else:
                print("âŒ No RCA data found")
                self.data_available = False

        except Exception as e:
            print(f"âŒ Error accessing RCA data: {str(e)}")
            self.data_available = False

    def search_similar_incidents(self, query: str, num_results: int = 5) -> Dict[str, Any]:
        """Search for similar incidents using text matching"""

        if not self.data_available:
            return {"error": "RCA data not available"}

        print(f"ðŸ” Searching for: '{query}'")

        try:
            rca_df = spark.table(self.rca_table)

            # Extract search terms
            search_terms = query.lower().split()

            # Build search conditions using correct column names
            search_conditions = None
            for term in search_terms:
                term_condition = (
                    col("rca_analysis").contains(term) |
                    col("resolution_recommendations").contains(term) |
                    col("root_cause_category").contains(term) |
                    col("recommended_operation").contains(term) |
                    col("incident_priority").contains(term)
                )

                if search_conditions is None:
                    search_conditions = term_condition
                else:
                    search_conditions = search_conditions | term_condition

            # Execute search
            if search_conditions is not None:
                results_df = rca_df.filter(search_conditions).limit(num_results)
            else:
                # Fallback: return recent records
                results_df = rca_df.orderBy(col("created_timestamp").desc()).limit(num_results)

            # Collect results
            results = results_df.collect()

            # Process results using correct column names
            processed_results = []
            for i, row in enumerate(results, 1):
                processed_result = {
                    "rank": i,
                    "incident_id": row["rca_id"] if "rca_id" in row else "Unknown",
                    "priority": row["incident_priority"] if "incident_priority" in row else "Unknown",
                    "operation": row["recommended_operation"] if "recommended_operation" in row else "Unknown",
                    "root_cause_category": row["root_cause_category"] if "root_cause_category" in row else "Unknown",
                    "rca_analysis": row["rca_analysis"] if "rca_analysis" in row else "N/A",
                    "recommendations": row["resolution_recommendations"] if "resolution_recommendations" in row else "N/A",
                    "confidence": row["analysis_confidence"] if "analysis_confidence" in row else "N/A",
                    "method": row["analysis_method"] if "analysis_method" in row else "N/A"
                }
                processed_results.append(processed_result)

            print(f"âœ… Found {len(processed_results)} matching incidents")

            return {
                "query": query,
                "search_method": "text_matching",
                "results_count": len(processed_results),
                "results": processed_results,
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            return {
                "query": query,
                "error": f"Search failed: {str(e)}"
            }

    def generate_ai_response(self, query: str, search_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate AI response using Foundation Model"""

        if "error" in search_results:
            return {"error": search_results["error"]}

        if search_results["results_count"] == 0:
            return {
                "query": query,
                "response": "I couldn't find any similar incidents in our knowledge base for your query. Please try rephrasing your question or provide more specific technical details.",
                "confidence": "low",
                "sources": []
            }

        try:
            # Build context from search results
            context_parts = []
            sources = []

            for result in search_results["results"][:3]:  # Use top 3 results
                context_part = f"""
Incident: {result['root_cause_category']} - {result['operation']} (Priority: {result['priority']})
RCA Analysis: {result['rca_analysis'][:300]}...
Recommendations: {result['recommendations'][:200]}...
Method: {result['method']} (Confidence: {result['confidence']})
                """.strip()
                context_parts.append(context_part)

                sources.append({
                    "incident_id": result['incident_id'],
                    "root_cause_category": result['root_cause_category'],
                    "operation": result['operation'],
                    "priority": result['priority']
                })

            context = "\n\n---\n\n".join(context_parts)

            # Create RAG prompt
            rag_prompt = f"""You are an expert network engineer analyzing incident reports. Based on the following similar incidents from our knowledge base, provide a comprehensive answer to the user's question.

User Question: {query}

Similar Incidents from Knowledge Base:
{context}

Instructions:
1. Provide a clear, actionable answer based on the similar incidents
2. Highlight common patterns and root causes
3. Suggest specific troubleshooting steps or solutions
4. Mention if there are severity considerations
5. Be concise but comprehensive
6. If multiple components are involved, address them systematically

Answer:"""

            # Try to use Foundation Model
            try:
                client = mlflow.deployments.get_deploy_client("databricks")

                response = client.predict(
                    endpoint=self.foundation_model,
                    inputs={
                        "messages": [
                            {"role": "user", "content": rag_prompt}
                        ],
                        "temperature": 0.3,
                        "max_tokens": 1000
                    }
                )

                ai_response = response.choices[0].message.content

                return {
                    "query": query,
                    "response": ai_response,
                    "method": "ai_generated",
                    "confidence": "high",
                    "sources": sources,
                    "context_incidents": len(search_results["results"]),
                    "timestamp": datetime.now().isoformat()
                }

            except Exception as ai_error:
                print(f"âš ï¸ AI response failed, using structured response: {str(ai_error)}")

                # Fallback to structured response
                structured_response = self._generate_structured_response(query, search_results["results"])

                return {
                    "query": query,
                    "response": structured_response,
                    "method": "structured_fallback",
                    "confidence": "medium",
                    "sources": sources,
                    "context_incidents": len(search_results["results"]),
                    "timestamp": datetime.now().isoformat()
                }

        except Exception as e:
            return {
                "query": query,
                "error": f"Failed to generate response: {str(e)}"
            }

    def _generate_structured_response(self, query: str, results: List[Dict]) -> str:
        """Generate structured response when AI is not available"""
        if not results:
            return "No similar incidents found in our knowledge base."

        top_result = results[0]

        response_parts = [
            f"Based on similar incidents in our network fault database:",
            f"",
            f"ðŸ” **Most Similar Incident:**",
            f"   Category: {top_result['root_cause_category']}",
            f"   Operation: {top_result['operation']}",
            f"   Priority: {top_result['priority']}",
            f"",
            f"ðŸŽ¯ **RCA Analysis:**",
            f"   {top_result['rca_analysis'][:300]}{'...' if len(top_result['rca_analysis']) > 300 else ''}",
            f"",
            f"ðŸ’¡ **Recommended Actions:**",
            f"   {top_result['recommendations'][:300]}{'...' if len(top_result['recommendations']) > 300 else ''}"
        ]

        if len(results) > 1:
            response_parts.extend([
                f"",
                f"ðŸ“Š **Additional Context:** Found {len(results)} similar incidents with related patterns.",
            ])

        return "\n".join(response_parts)

    def intelligent_search(self, query: str, num_results: int = 5) -> Dict[str, Any]:
        """Complete intelligent search with AI response"""
        print(f"ðŸ¤– Processing intelligent search: '{query}'")

        # Step 1: Text-based similarity search
        search_results = self.search_similar_incidents(query, num_results)

        # Step 2: Generate AI response
        ai_response = self.generate_ai_response(query, search_results)

        # Step 3: Combine results
        final_result = {
            "query": query,
            "search_results": search_results,
            "ai_response": ai_response,
            "timestamp": datetime.now().isoformat()
        }

        # Step 4: Store search history
        self.search_history.append({
            "query": query,
            "timestamp": datetime.now().isoformat(),
            "results_found": search_results.get("results_count", 0),
            "method": ai_response.get("method", "unknown")
        })

        return final_result

# COMMAND ----------

# Initialize the Basic RAG search engine
print("ðŸš€ Initializing Basic RAG Search Engine...")
basic_rag_engine = BasicRAGSearchEngine()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Basic RAG Search

# COMMAND ----------

def test_basic_rag_search():
    """Test basic RAG search capabilities"""
    print("ðŸ§ª TESTING BASIC RAG SEARCH")
    print("=" * 50)

    # Test queries
    test_queries = [
        "router interface down causing connectivity issues",
        "high CPU utilization network device performance",
        "BGP routing protocol failure troubleshooting",
        "how to fix firewall configuration problems"
    ]

    for i, query in enumerate(test_queries, 1):
        print(f"\nðŸ” Test Query {i}: '{query}'")
        print("-" * 40)

        try:
            result = basic_rag_engine.intelligent_search(query, num_results=3)

            # Display results
            search_info = result["search_results"]
            ai_info = result["ai_response"]

            print(f"ðŸ“Š Search Results: {search_info.get('results_count', 0)} incidents found")

            if "error" not in ai_info:
                print(f"ðŸ¤– AI Response Method: {ai_info.get('method', 'unknown')}")
                print(f"ðŸŽ¯ Confidence: {ai_info.get('confidence', 'unknown')}")
                print(f"ðŸ“‹ Sources: {len(ai_info.get('sources', []))} incident references")

                # Show response preview
                response_preview = ai_info.get('response', 'No response generated')[:200]
                print(f"ðŸ’¬ Response Preview: {response_preview}...")
            else:
                print(f"âŒ AI Error: {ai_info['error']}")

        except Exception as e:
            print(f"âŒ Test failed: {str(e)}")

        print()

# Run test
test_basic_rag_search()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive Search Interface

# COMMAND ----------

def demo_interactive_search():
    """Demonstrate interactive search capabilities"""
    print("ðŸ’¬ DEMO: INTERACTIVE BASIC RAG SEARCH")
    print("=" * 50)

    demo_queries = [
        "Show me router failures with high severity",
        "What causes network performance issues",
        "How to troubleshoot BGP neighbor down"
    ]

    for query in demo_queries:
        print(f"\nðŸŽ¯ Query: '{query}'")
        print("-" * 30)

        result = basic_rag_engine.intelligent_search(query, num_results=2)
        ai_response = result["ai_response"]

        if "error" not in ai_response:
            print(f"ðŸ¤– Response:")
            print(ai_response["response"][:500] + "..." if len(ai_response["response"]) > 500 else ai_response["response"])

            if ai_response.get("sources"):
                print(f"\nðŸ“š Sources: {len(ai_response['sources'])} incidents")
        else:
            print(f"âŒ Error: {ai_response['error']}")

        print()

# Run demo
demo_interactive_search()

# COMMAND ----------

print("âœ… Basic RAG Search System Ready!")
print("ðŸ’¡ This system provides intelligent search without Vector Search requirements")
print("ðŸŽ¯ Perfect for free trial environments with limited Vector Search access")
