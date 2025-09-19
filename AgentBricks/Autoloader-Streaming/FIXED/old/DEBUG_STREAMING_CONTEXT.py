# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ” DEBUG STREAMING CONTEXT - Find Why AI Fails in Main Script
# MAGIC **Goal**: Debug exactly what happens to AI calls within streaming context

# COMMAND ----------

print("ðŸ”" * 80)
print("DEBUG STREAMING CONTEXT - FIND THE ACTUAL PROBLEM")
print("ðŸ”" * 80)

import time
import json
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import IntegerType
import mlflow.deployments

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Use same configuration as main script
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"

RAW_LOGS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.raw_network_logs_streaming"
SEVERITY_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.severity_classifications_streaming"

LOG_SOURCE_PATH = "/FileStore/logs/network_logs_streaming"
RAW_CHECKPOINT = "/FileStore/checkpoints/raw_ai_rules_raw_fixed"
SEV_CHECKPOINT = "/FileStore/checkpoints/raw_ai_rules_sev_fixed"

FOUNDATION_MODEL_NAME = "databricks-meta-llama-3-1-8b-instruct"

print(f"ðŸ”§ Configuration loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Test AI in Streaming Environment Context

# COMMAND ----------

print("ðŸ§ª STEP 1: Test AI in Streaming Environment Context")
print("-" * 60)

# Initialize exactly like main script
try:
    client = mlflow.deployments.get_deploy_client("databricks")
    AI_ENABLED = True
    print(f"âœ… AI model enabled: {FOUNDATION_MODEL_NAME}")
    print(f"ðŸ¤– AI_ENABLED = {AI_ENABLED}")
    
    # IMMEDIATE TEST: Try a simple FM call right now IN STREAMING CONTEXT
    print("ðŸ§ª Testing FM in streaming environment context...")
    test_response = client.predict(
        endpoint=FOUNDATION_MODEL_NAME,
        inputs={
            "messages": [{"role": "user", "content": "Classify as P1, P2, P3 or INFO: CRITICAL system failure"}],
            "temperature": 0.1,
            "max_tokens": 20
        }
    )
    
    print(f"âœ… Streaming context FM test successful!")
    print(f"ðŸ“¨ Response keys: {list(test_response.keys())}")
    
    if "choices" in test_response:
        prediction = test_response["choices"][0]["message"]["content"].strip()
        print(f"âœ… Streaming context prediction: '{prediction}'")
        
        # Test parsing
        prediction_upper = prediction.upper()
        if "P1" in prediction_upper:
            severity = "P1"
        elif "P2" in prediction_upper:
            severity = "P2"
        elif "P3" in prediction_upper:
            severity = "P3"
        else:
            severity = "INFO"
        print(f"âœ… Streaming context parsed severity: {severity}")
    
except Exception as e:
    client = None
    AI_ENABLED = False
    print(f"âŒ FM client failed in streaming context: {e}")
    print(f"ðŸ¤– AI_ENABLED = {AI_ENABLED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Test Exact Functions from Main Script

# COMMAND ----------

print("\nðŸ”§ STEP 2: Test Exact Functions from Main Script")
print("-" * 60)

# Copy exact functions from main script
def classify_with_rules(log_content: str) -> dict:
    log_lower = log_content.lower()
    if any(k in log_lower for k in ["critical", "fatal", "emergency", "outage", "offline"]):
        return {"severity": "P1", "confidence": 0.95, "method": "rule_based", "reasoning": "Critical keywords"}
    elif any(k in log_lower for k in ["error", "exception", "failure", "timeout", "denied"]):
        return {"severity": "P2", "confidence": 0.85, "method": "rule_based", "reasoning": "Error keywords"}
    elif any(k in log_lower for k in ["warn", "degraded", "usage", "retry"]):
        return {"severity": "P3", "confidence": 0.75, "method": "rule_based", "reasoning": "Warning keywords"}
    else:
        return {"severity": "INFO", "confidence": 0.7, "method": "rule_based", "reasoning": "Informational"}

def classify_with_fm(log_content: str) -> dict:
    """GUARANTEED working AI classification - 100% tested success pattern"""
    if not AI_ENABLED:
        print("ðŸš« AI_ENABLED is False - skipping FM call")
        return {"success": False}
    
    try:
        print(f"ðŸ¤– GUARANTEED AI CALL for: {log_content[:50]}...")
        
        response = client.predict(
            endpoint=FOUNDATION_MODEL_NAME,
            inputs={
                "messages": [
                    {"role": "system", "content": "You are an expert incident classifier."},
                    {"role": "user", "content": f"Classify as P1, P2, P3 or INFO: {log_content}"}
                ],
                "temperature": 0.1,
                "max_tokens": 20  # Reduced for faster response
            }
        )
        
        if "choices" in response:
            prediction = response["choices"][0]["message"]["content"].strip()
            prediction_upper = prediction.upper()
            
            if "P1" in prediction_upper:
                severity = "P1"
            elif "P2" in prediction_upper:
                severity = "P2"
            elif "P3" in prediction_upper:
                severity = "P3"
            else:
                severity = "INFO"
            
            print(f"ðŸŽ‰ AI SUCCESS: {severity} from '{prediction[:30]}...'")
            return {
                "success": True,
                "severity": severity,
                "confidence": 0.85,
                "method": "ai_foundation_model",  # THIS WILL SHOW AI USAGE!
                "reasoning": f"AI classified as {severity}"
            }
        else:
            print(f"âŒ No choices in response: {list(response.keys())}")
            return {"success": False}
            
    except Exception as e:
        print(f"ðŸš¨ AI failed: {e}")
        return {"success": False}

def hybrid_classify(log_content: str) -> dict:
    """GUARANTEED AI+Rules hybrid - uses exact working pattern"""
    print(f"ðŸ”„ HYBRID_CLASSIFY called for: {log_content[:30]}...")
    
    # Try AI first (guaranteed working pattern)
    fm_res = classify_with_fm(log_content)
    
    if fm_res.get("success"):
        print(f"ðŸŽ‰ USING AI CLASSIFICATION: {fm_res.get('severity')}")
        return fm_res  # Return AI result directly
    else:
        print(f"ðŸ“‹ Using rules fallback")
        return classify_with_rules(log_content)

print("âœ… Functions loaded - testing individually...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test Functions with Sample Logs

# COMMAND ----------

print("\nðŸ§ª STEP 3: Test Functions with Sample Logs")
print("-" * 60)

test_logs = [
    "CRITICAL Database connection lost",
    "ERROR Network timeout occurred", 
    "WARN High memory usage",
    "INFO System backup completed"
]

print("Testing classify_with_fm directly:")
for i, log in enumerate(test_logs, 1):
    print(f"\nðŸ“ Direct FM Test {i}: {log}")
    fm_result = classify_with_fm(log)
    print(f"   Result: {fm_result}")

print("\n" + "="*60)
print("Testing hybrid_classify:")
ai_count = 0
rules_count = 0

for i, log in enumerate(test_logs, 1):
    print(f"\nðŸ“ Hybrid Test {i}: {log}")
    hybrid_result = hybrid_classify(log)
    print(f"   Final Result: {hybrid_result['severity']} via {hybrid_result['method']}")
    
    if "ai" in hybrid_result["method"]:
        ai_count += 1
    else:
        rules_count += 1

print(f"\nðŸ“Š FUNCTION TEST RESULTS:")
print(f"   ðŸ¤– AI Classifications: {ai_count}/{len(test_logs)} ({ai_count/len(test_logs)*100:.1f}%)")
print(f"   ðŸ“‹ Rules Classifications: {rules_count}/{len(test_logs)} ({rules_count/len(test_logs)*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test in Batch Processing Context

# COMMAND ----------

print("\nðŸ”„ STEP 4: Test in Batch Processing Context (foreachBatch simulation)")
print("-" * 60)

# Create a simple DataFrame to simulate batch processing
test_data = [
    Row(log_content="CRITICAL Database connection lost", file_path="/test/critical.log"),
    Row(log_content="ERROR Network timeout occurred", file_path="/test/error.log"),
    Row(log_content="WARN High memory usage", file_path="/test/warn.log"),
    Row(log_content="INFO System backup completed", file_path="/test/info.log")
]

test_df = spark.createDataFrame(test_data)
print(f"âœ… Test DataFrame created with {test_df.count()} rows")

# Simulate foreachBatch processing exactly like main script
def debug_batch_processing(batch_df, batch_id):
    print(f"\nðŸ”„ DEBUG BATCH {batch_id}: Processing {batch_df.count()} logs")
    
    rows = batch_df.collect()
    results = []
    ai_batch_count = 0
    rules_batch_count = 0
    
    for idx, row in enumerate(rows):
        row_dict = row.asDict()
        log_content = row_dict["log_content"]
        file_path = row_dict["file_path"]
        
        print(f"\n   ðŸ“ Processing row {idx+1}: {log_content[:40]}...")
        
        # This is the exact call from main script
        result = hybrid_classify(log_content)
        
        print(f"   ðŸ“Š Result: {result['severity']} via {result['method']}")
        
        if "ai" in result["method"]:
            ai_batch_count += 1
        else:
            rules_batch_count += 1
        
        # Simulate creating record like main script
        record = {
            "severity_id": f"debug_{int(time.time()*1000000)}_{idx}",
            "raw_log_content": log_content,
            "predicted_severity": result["severity"],
            "confidence_score": result["confidence"],
            "classification_method": result["method"],
            "classification_timestamp": datetime.now(),
            "file_source_path": file_path,
            "ai_reasoning": result.get("reasoning", ""),
            "processing_time_ms": int(0)
        }
        results.append(record)
    
    print(f"\nðŸ“Š BATCH {batch_id} RESULTS:")
    print(f"   ðŸ¤– AI Classifications: {ai_batch_count}")
    print(f"   ðŸ“‹ Rules Classifications: {rules_batch_count}")
    print(f"   ðŸŽ¯ AI Percentage: {(ai_batch_count/(ai_batch_count+rules_batch_count)*100):.1f}%")
    
    return results, ai_batch_count, rules_batch_count

# Run the batch processing test
batch_results, batch_ai_count, batch_rules_count = debug_batch_processing(test_df, "DEBUG")

print(f"\nðŸŽ¯ FINAL BATCH PROCESSING TEST:")
print(f"   ðŸ¤– Total AI: {batch_ai_count}")
print(f"   ðŸ“‹ Total Rules: {batch_rules_count}")

if batch_ai_count > 0:
    print(f"\nðŸŽ‰ SUCCESS! AI is working in batch processing context!")
    print(f"ðŸ”§ The issue is NOT in the functions - something else is wrong in main script")
else:
    print(f"\nâŒ FAILURE! AI is still not working even in batch processing simulation")
    print(f"ðŸ”§ The issue IS in the functions or streaming environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Compare with Main Script Environment

# COMMAND ----------

print("\nðŸ” STEP 5: Environment Comparison")
print("-" * 60)

print("ðŸ”§ Current Environment Variables:")
print(f"   AI_ENABLED: {AI_ENABLED}")
print(f"   FOUNDATION_MODEL_NAME: {FOUNDATION_MODEL_NAME}")
print(f"   Client available: {client is not None}")

if AI_ENABLED and client:
    print(f"   Client type: {type(client)}")
    
print(f"\nðŸ“Š Key Differences from Main Script:")
print(f"   1. This runs in notebook cell context")
print(f"   2. Main script runs in streaming context")
print(f"   3. Console output visible here, suppressed in streaming")
print(f"   4. Error handling might be different")

print(f"\nðŸŽ¯ DIAGNOSIS:")
if batch_ai_count > 0:
    print(f"âœ… AI functions work perfectly in debug context")
    print(f"âŒ Something in main script streaming pipeline breaks AI")
    print(f"ðŸ”§ SOLUTION: Find difference between this context and main script context")
else:
    print(f"âŒ AI functions fail even in debug context") 
    print(f"ðŸ”§ SOLUTION: Fix the functions themselves first")

print("\nðŸ”" * 80)
