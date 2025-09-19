# Databricks notebook source
# MAGIC %md
# MAGIC # FINAL AUTH DEBUG - Test Different Authentication Approaches
# MAGIC **Goal**: Try different MLflow authentication methods for streaming

# COMMAND ----------

import time
from datetime import datetime
from pyspark.sql import SparkSession
import mlflow
import mlflow.deployments
import os

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Test different authentication approaches
FOUNDATION_MODEL_NAME = "databricks-meta-llama-3-1-8b-instruct"

print("Testing different MLflow authentication approaches...")
print("=" * 60)

# COMMAND ----------

# Approach 1: Standard client (what we've been using)
print("ðŸ§ª Approach 1: Standard MLflow client")
try:
    client1 = mlflow.deployments.get_deploy_client("databricks")
    print(f"âœ… Standard client created: {type(client1)}")
    
    # Test call
    response1 = client1.predict(
        endpoint=FOUNDATION_MODEL_NAME,
        inputs={
            "messages": [{"role": "user", "content": "Test message"}],
            "temperature": 0.1,
            "max_tokens": 10
        }
    )
    print(f"âœ… Standard client prediction successful")
except Exception as e:
    print(f"âŒ Standard client failed: {e}")

# COMMAND ----------

# Approach 2: Set MLflow tracking URI explicitly
print("\nðŸ§ª Approach 2: Explicit tracking URI")
try:
    mlflow.set_tracking_uri("databricks")
    client2 = mlflow.deployments.get_deploy_client("databricks")
    print(f"âœ… Explicit URI client created: {type(client2)}")
    
    # Test call
    response2 = client2.predict(
        endpoint=FOUNDATION_MODEL_NAME,
        inputs={
            "messages": [{"role": "user", "content": "Test message"}],
            "temperature": 0.1,
            "max_tokens": 10
        }
    )
    print(f"âœ… Explicit URI client prediction successful")
except Exception as e:
    print(f"âŒ Explicit URI client failed: {e}")

# COMMAND ----------

# Approach 3: Use mlflow.llm.invoke directly (if available)
print("\nðŸ§ª Approach 3: Direct MLflow LLM invoke")
try:
    # Check if we can use direct invoke
    import mlflow.llms
    print(f"âœ… MLflow LLMs module available")
    
    # This might be a different way to call Foundation Models
    # direct_response = mlflow.llms.invoke(
    #     model=FOUNDATION_MODEL_NAME,
    #     inputs="Test message"
    # )
    print(f"â„¹ï¸  Direct invoke approach would need different syntax")
except Exception as e:
    print(f"âŒ Direct invoke not available: {e}")

# COMMAND ----------

# Approach 4: Test different endpoint formats
print("\nðŸ§ª Approach 4: Different endpoint naming")
endpoint_variations = [
    "databricks-meta-llama-3-1-8b-instruct",
    f"/serving-endpoints/{FOUNDATION_MODEL_NAME}",
    f"serving-endpoints/{FOUNDATION_MODEL_NAME}"
]

for endpoint in endpoint_variations:
    try:
        client4 = mlflow.deployments.get_deploy_client("databricks")
        response4 = client4.predict(
            endpoint=endpoint,
            inputs={
                "messages": [{"role": "user", "content": "Test"}],
                "temperature": 0.1,
                "max_tokens": 10
            }
        )
        print(f"âœ… Endpoint variation '{endpoint}' successful")
        break
    except Exception as e:
        print(f"âŒ Endpoint '{endpoint}' failed: {str(e)[:100]}...")

# COMMAND ----------

# Approach 5: Environment variables check
print("\nðŸ§ª Approach 5: Environment check")
env_vars = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN', 'MLFLOW_TRACKING_URI']
for var in env_vars:
    value = os.environ.get(var)
    if value:
        print(f"âœ… {var}: {'*' * 10}")  # Mask actual values
    else:
        print(f"âŒ {var}: Not set")

# COMMAND ----------

# Approach 6: Test in simulated streaming context (using map function)
print("\nðŸ§ª Approach 6: Simulated streaming context")

def test_ai_in_partition(iterator):
    """Test AI call inside a partition - simulates streaming context"""
    results = []
    
    for row in iterator:
        try:
            # Re-create client inside partition (like streaming)
            partition_client = mlflow.deployments.get_deploy_client("databricks")
            
            response = partition_client.predict(
                endpoint=FOUNDATION_MODEL_NAME,
                inputs={
                    "messages": [{"role": "user", "content": f"Classify: {row.log_content}"}],
                    "temperature": 0.1,
                    "max_tokens": 10
                }
            )
            
            if "choices" in response:
                result = "AI_SUCCESS"
            else:
                result = "AI_NO_CHOICES"
                
        except Exception as e:
            result = f"AI_FAILED: {str(e)[:50]}"
        
        results.append((row.log_content, result))
    
    return iter(results)

# Create test data
from pyspark.sql import Row
test_data = [
    Row(log_content="CRITICAL test failure"),
    Row(log_content="ERROR network timeout")
]
test_df = spark.createDataFrame(test_data)

# Test in partition context
try:
    partition_results = test_df.mapPartitions(test_ai_in_partition).collect()
    
    print(f"ðŸ“Š Partition test results:")
    for log_content, result in partition_results:
        print(f"   {log_content[:30]}... -> {result}")
        
    # Count successes
    successes = sum(1 for _, result in partition_results if "AI_SUCCESS" in result)
    print(f"ðŸŽ¯ Partition AI success rate: {successes}/{len(partition_results)} ({successes/len(partition_results)*100:.1f}%)")
    
except Exception as e:
    print(f"âŒ Partition test failed: {e}")

print("=" * 60)
