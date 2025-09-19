# Databricks notebook source
# MAGIC %md
# MAGIC # DEBUG TO TABLE - Capture Debug Info in Delta Table
# MAGIC **Goal**: Write debug info to table since console output is invisible in streaming

# COMMAND ----------

import time
import json
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import mlflow.deployments

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Configuration
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"
RAW_LOGS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.network_logs_streaming"
SEVERITY_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.severity_classifications_streaming"
DEBUG_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.debug_ai_streaming"  # New debug table
FOUNDATION_MODEL_NAME = "databricks-meta-llama-3-1-8b-instruct"

# COMMAND ----------

# Create debug table to capture what's happening
print("Creating debug table...")
spark.sql(f"DROP TABLE IF EXISTS {DEBUG_TABLE}")
spark.sql(f"""
CREATE TABLE {DEBUG_TABLE} (
    debug_id STRING,
    batch_id STRING,
    log_content STRING,
    ai_enabled BOOLEAN,
    client_available BOOLEAN,
    ai_call_attempted BOOLEAN,
    ai_call_success BOOLEAN,
    ai_error STRING,
    final_method STRING,
    final_severity STRING,
    debug_timestamp TIMESTAMP
) USING DELTA
""")

print("âœ… Debug table created")

# COMMAND ----------

# Initialize AI exactly like main script
global client, AI_ENABLED

try:
    client = mlflow.deployments.get_deploy_client("databricks")
    AI_ENABLED = True
    print(f"âœ… AI initialized: {FOUNDATION_MODEL_NAME}")
except Exception as e:
    client = None
    AI_ENABLED = False
    print(f"âŒ AI failed: {e}")

# COMMAND ----------

# Copy exact functions with debug table logging
def classify_with_fm_debug(log_content: str, debug_id: str) -> dict:
    global client, AI_ENABLED
    
    ai_call_attempted = False
    ai_call_success = False
    ai_error = ""
    
    if not AI_ENABLED:
        ai_error = "AI_ENABLED is False"
        return {"success": False, "debug": {"ai_call_attempted": ai_call_attempted, "ai_call_success": ai_call_success, "ai_error": ai_error}}
    
    if client is None:
        ai_error = "Client is None"
        return {"success": False, "debug": {"ai_call_attempted": ai_call_attempted, "ai_call_success": ai_call_success, "ai_error": ai_error}}
    
    try:
        ai_call_attempted = True
        
        response = client.predict(
            endpoint=FOUNDATION_MODEL_NAME,
            inputs={
                "messages": [
                    {"role": "system", "content": "You are an expert incident classifier."},
                    {"role": "user", "content": f"Classify as P1, P2, P3 or INFO: {log_content}"}
                ],
                "temperature": 0.1,
                "max_tokens": 20
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
            
            ai_call_success = True
            return {
                "success": True,
                "severity": severity,
                "confidence": 0.85,
                "method": "ai_foundation_model",
                "reasoning": f"AI classified as {severity}",
                "debug": {"ai_call_attempted": ai_call_attempted, "ai_call_success": ai_call_success, "ai_error": ai_error}
            }
        else:
            ai_error = "No choices in response"
            return {"success": False, "debug": {"ai_call_attempted": ai_call_attempted, "ai_call_success": ai_call_success, "ai_error": ai_error}}
            
    except Exception as e:
        ai_error = str(e)
        return {"success": False, "debug": {"ai_call_attempted": ai_call_attempted, "ai_call_success": ai_call_success, "ai_error": ai_error}}

def classify_with_rules_debug(log_content: str) -> dict:
    log_lower = log_content.lower()
    if any(k in log_lower for k in ["critical", "fatal", "emergency", "outage", "offline"]):
        return {"severity": "P1", "confidence": 0.95, "method": "rule_based", "reasoning": "Critical keywords"}
    elif any(k in log_lower for k in ["error", "exception", "failure", "timeout", "denied"]):
        return {"severity": "P2", "confidence": 0.85, "method": "rule_based", "reasoning": "Error keywords"}
    elif any(k in log_lower for k in ["warn", "degraded", "usage", "retry"]):
        return {"severity": "P3", "confidence": 0.75, "method": "rule_based", "reasoning": "Warning keywords"}
    else:
        return {"severity": "INFO", "confidence": 0.7, "method": "rule_based", "reasoning": "Informational"}

def hybrid_classify_debug(log_content: str, debug_id: str) -> dict:
    # Try AI first
    fm_res = classify_with_fm_debug(log_content, debug_id)
    
    if fm_res.get("success"):
        return fm_res
    else:
        # Use rules fallback
        rules_res = classify_with_rules_debug(log_content)
        rules_res["debug"] = fm_res.get("debug", {})
        return rules_res

print("âœ… Debug functions loaded")

# COMMAND ----------

# Debug foreachBatch that writes to table
def debug_foreachbatch_with_table(batch_df, batch_id):
    global client, AI_ENABLED
    
    if batch_df.count() == 0:
        return
    
    rows = batch_df.collect()
    debug_records = []
    
    for idx, row in enumerate(rows):
        row_dict = row.asDict()
        log_content = row_dict.get("log_content", "").strip()
        
        if not log_content:
            continue
            
        debug_id = f"debug_{int(time.time()*1000000)}_{idx}"
        
        # Call hybrid classification with debug
        result = hybrid_classify_debug(log_content, debug_id)
        
        # Extract debug info
        debug_info = result.get("debug", {})
        
        # Create debug record
        debug_record = {
            "debug_id": debug_id,
            "batch_id": str(batch_id),
            "log_content": log_content[:100],  # Truncate for storage
            "ai_enabled": AI_ENABLED,
            "client_available": client is not None,
            "ai_call_attempted": debug_info.get("ai_call_attempted", False),
            "ai_call_success": debug_info.get("ai_call_success", False),
            "ai_error": debug_info.get("ai_error", ""),
            "final_method": result.get("method", ""),
            "final_severity": result.get("severity", ""),
            "debug_timestamp": datetime.now()
        }
        debug_records.append(debug_record)
    
    # Write debug records to table
    if debug_records:
        debug_df = spark.createDataFrame(debug_records)
        debug_df.write.format("delta").mode("append").saveAsTable(DEBUG_TABLE)

print("âœ… Debug foreachBatch ready")

# COMMAND ----------

# Run one-time debug stream
print("ðŸ” Starting debug stream to capture AI behavior...")

debug_stream = (spark.readStream
    .format("delta")
    .table(RAW_LOGS_TABLE)
    .writeStream
    .foreachBatch(debug_foreachbatch_with_table)
    .option("checkpointLocation", "/FileStore/checkpoints/debug_ai_stream")
    .trigger(once=True)  # Run once to capture current state
    .start())

debug_stream.awaitTermination()

print("âœ… Debug stream completed")

# COMMAND ----------

# Analyze debug results
print("ðŸ” ANALYZING DEBUG RESULTS...")
print("=" * 60)

debug_results = spark.table(DEBUG_TABLE).orderBy("debug_timestamp").collect()

print(f"ðŸ“Š Total debug records: {len(debug_results)}")

ai_enabled_count = sum(1 for r in debug_results if r.ai_enabled)
client_available_count = sum(1 for r in debug_results if r.client_available) 
ai_attempted_count = sum(1 for r in debug_results if r.ai_call_attempted)
ai_success_count = sum(1 for r in debug_results if r.ai_call_success)

print(f"ðŸ”§ AI_ENABLED: {ai_enabled_count}/{len(debug_results)} ({ai_enabled_count/len(debug_results)*100:.1f}%)")
print(f"ðŸ”§ Client Available: {client_available_count}/{len(debug_results)} ({client_available_count/len(debug_results)*100:.1f}%)")
print(f"ðŸ§ª AI Calls Attempted: {ai_attempted_count}/{len(debug_results)} ({ai_attempted_count/len(debug_results)*100:.1f}%)")
print(f"âœ… AI Calls Successful: {ai_success_count}/{len(debug_results)} ({ai_success_count/len(debug_results)*100:.1f}%)")

# Show method breakdown
method_counts = {}
for r in debug_results:
    method = r.final_method
    method_counts[method] = method_counts.get(method, 0) + 1

print(f"\nðŸ“Š FINAL METHOD BREAKDOWN:")
for method, count in method_counts.items():
    percentage = (count / len(debug_results)) * 100
    print(f"   {method}: {count} ({percentage:.1f}%)")

# Show errors if any
errors = [r for r in debug_results if r.ai_error]
if errors:
    print(f"\nâŒ AI ERRORS FOUND ({len(errors)} errors):")
    for error in errors[:5]:  # Show first 5 errors
        print(f"   {error.ai_error}")
else:
    print(f"\nâœ… NO AI ERRORS FOUND")

# Show sample records
print(f"\nðŸ“ SAMPLE DEBUG RECORDS:")
for i, record in enumerate(debug_results[:3]):
    print(f"   Record {i+1}:")
    print(f"     Log: {record.log_content[:50]}...")
    print(f"     AI Enabled: {record.ai_enabled}")
    print(f"     Client Available: {record.client_available}")
    print(f"     AI Attempted: {record.ai_call_attempted}")
    print(f"     AI Success: {record.ai_call_success}")
    print(f"     AI Error: {record.ai_error}")
    print(f"     Final Method: {record.final_method}")
    print(f"     Final Severity: {record.final_severity}")

print("=" * 60)
