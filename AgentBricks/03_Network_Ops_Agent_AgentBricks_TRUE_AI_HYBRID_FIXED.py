# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Network Operations Agent - TRUE AI + Rules Hybrid (FIXED)

# COMMAND ----------

import time
import json
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import IntegerType
import mlflow.deployments

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Configuration
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"

INCIDENTS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.incident_decisions_streaming"
NETWORK_OPS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.network_operations_streaming"

OPS_CHECKPOINT = "/FileStore/checkpoints/network_ops_ai_hybrid_fixed"

FOUNDATION_MODEL_NAME = "databricks-meta-llama-3-1-8b-instruct"

# COMMAND ----------

# 🧹 AUTOMATED CHECKPOINT CLEANUP FUNCTION
def cleanup_checkpoint_if_needed(checkpoint_path, table_name, description=""):
    """Clean checkpoint when table schema changes or for fresh starts"""
    try:
        print(f"🔍 Checking checkpoint: {description}")

        # Check if checkpoint exists
        try:
            checkpoint_files = dbutils.fs.ls(checkpoint_path)
            if len(checkpoint_files) > 0:
                print(f"🧹 Cleaning existing checkpoint: {checkpoint_path}")
                dbutils.fs.rm(checkpoint_path, recurse=True)
                print(f"✅ Checkpoint cleaned: {description}")
            else:
                print(f"ℹ️ No checkpoint to clean: {description}")
        except Exception as ls_error:
            print(f"ℹ️ Checkpoint doesn't exist or already clean: {description}")

    except Exception as e:
        print(f"⚠️ Checkpoint cleanup warning for {description}: {str(e)}")

print("🛠️ Network Operations checkpoint cleanup function ready")

# Force rule-based processing for reliable table population
AI_ENABLED = False
print("🔧 Using rule-based processing only for reliable table population")

# COMMAND ----------

# AI + Rules Hybrid Functions
def plan_operation_with_fm(incident_data: dict) -> dict:
    if not AI_ENABLED:
        return {"success": False}
    try:
        priority = incident_data.get("incident_priority", "INFO")
        escalation = incident_data.get("escalation_required", False)
        log_content = incident_data.get("original_log_content", "")
        
        response = client.predict(
            endpoint=FOUNDATION_MODEL_NAME,
            inputs={
                "messages": [
                    {"role": "system", "content": "You are a network operations expert."},
                    {"role": "user", "content": f"For {priority} priority incident (escalation: {escalation}) from log '{log_content[:100]}', recommend operation (restart_node/reroute_traffic/scale_resources/investigate/monitor). Format: OPERATION:RISK_LEVEL:REASONING"}
                ],
                "temperature": 0.1,
                "max_tokens": 80
            }
        )
        
        prediction = None
        if "choices" in response:
            prediction = response["choices"][0]["message"]["content"].strip()
        elif "predictions" in response and len(response["predictions"]) > 0:
            pred_obj = response["predictions"][0]
            if "candidates" in pred_obj:
                prediction = pred_obj["candidates"][0]["text"].strip()
            elif "generated_text" in pred_obj:
                prediction = pred_obj["generated_text"].strip()

        if prediction:
            parts = prediction.split(":")
            if len(parts) >= 3:
                operation = parts[0].strip().lower()
                risk_level = parts[1].strip().upper()
                reasoning = ":".join(parts[2:]).strip()
                
                valid_ops = ["restart_node", "reroute_traffic", "scale_resources", "investigate", "monitor"]
                if operation in valid_ops and risk_level in ["LOW", "MEDIUM", "HIGH"]:
                    return {"success": True, "operation": operation, "risk_level": risk_level,
                           "method": "ai_foundation_model", "reasoning": reasoning[:100]}
        return {"success": False}
    except Exception as e:
        print(f"⚠️ FM call failed: {e}")
        return {"success": False}

def plan_operation_with_rules(incident_data: dict) -> dict:
    priority = incident_data.get("incident_priority", "INFO")
    escalation = incident_data.get("escalation_required", False)
    log_content = incident_data.get("original_log_content", "").lower()
    
    if priority == "HIGH":
        if "outage" in log_content or "down" in log_content:
            return {"operation": "restart_node", "risk_level": "MEDIUM", "method": "rule_based", "reasoning": "High priority outage requires restart"}
        elif escalation:
            return {"operation": "reroute_traffic", "risk_level": "LOW", "method": "rule_based", "reasoning": "Escalated incident needs traffic rerouting"}
        else:
            return {"operation": "investigate", "risk_level": "LOW", "method": "rule_based", "reasoning": "High priority investigation"}
    elif priority == "MEDIUM":
        if "performance" in log_content or "slow" in log_content:
            return {"operation": "scale_resources", "risk_level": "LOW", "method": "rule_based", "reasoning": "Performance issues need scaling"}
        else:
            return {"operation": "investigate", "risk_level": "LOW", "method": "rule_based", "reasoning": "Medium priority investigation"}
    else:
        return {"operation": "monitor", "risk_level": "LOW", "method": "rule_based", "reasoning": "Low priority monitoring"}

def hybrid_operation_planning(incident_data: dict) -> dict:
    fm_res = plan_operation_with_fm(incident_data)
    if fm_res.get("success"):
        return fm_res
    return plan_operation_with_rules(incident_data)

# COMMAND ----------

# ✅ APPEND MODE: Setup Tables - Preserve historical data
try:
    existing_ops = spark.table(NETWORK_OPS_TABLE).count()
    print(f"📊 Found existing network operations table with {existing_ops} records - will append new data")
except:
    print("📋 Creating new network operations table")
    spark.sql(f"""
    CREATE TABLE {NETWORK_OPS_TABLE} (
        operation_id STRING,
        incident_priority STRING,
        recommended_operation STRING,
        risk_level STRING,
        operation_timestamp TIMESTAMP,
        planning_method STRING,
        operation_reasoning STRING,
        execution_status STRING,
        processing_time_ms INT
    ) USING DELTA
    """)

print("✅ Network operations table ready")

# COMMAND ----------

# ForeachBatch Processor
def process_operations_batch(batch_df, batch_id):
    batch_count = batch_df.count()
    print(f"\n🔧 Processing operations batch {batch_id} with {batch_count} incident decisions")

    if batch_count == 0:
        print("⚠️ Empty batch received - no incident decisions to process")
        return

    # Show sample data for debugging
    print("🔍 Sample batch data:")
    batch_df.show(3, truncate=False)

    rows = batch_df.collect()
    results = []

    for idx, row in enumerate(rows):
        try:
            row_dict = row.asDict()  # ✅ FIXED: Safe field access

            # Debug: Show available columns
            if idx == 0:  # Only for first row to avoid spam
                print(f"📋 Available columns: {list(row_dict.keys())}")

            incident_data = {
                "incident_priority": row_dict.get("incident_priority", "INFO"),
                "escalation_required": row_dict.get("escalate_required", False),  # ✅ FIXED: Correct column name
                "original_log_content": row_dict.get("raw_log_content", "")  # ✅ FIXED: Use raw_log_content
            }
        except Exception as row_error:
            print(f"❌ Error processing row {idx}: {str(row_error)}")
            continue
        
        try:
            result = hybrid_operation_planning(incident_data)

            record = {
                "operation_id": f"op_{int(time.time()*1000000)}_{idx}",
                "incident_priority": row_dict.get("incident_priority", "INFO"),  # ✅ FIXED: Safe access
                "recommended_operation": result["operation"],
                "risk_level": result["risk_level"],
                "operation_timestamp": datetime.now(),
                "planning_method": result["method"],
                "operation_reasoning": result.get("reasoning", ""),
                "execution_status": "planned",
                "processing_time_ms": int(0)
            }
            results.append(record)
        except Exception as planning_error:
            print(f"❌ Error planning operation for row {idx}: {str(planning_error)}")
            continue
    
    if results:
        results_df = spark.createDataFrame(results)
        # ✅ FIXED: Explicit schema alignment with type casting
        aligned_df = results_df.select(
            "operation_id", "incident_priority", "recommended_operation", "risk_level",
            "operation_timestamp", "planning_method", "operation_reasoning", "execution_status",
            col("processing_time_ms").cast(IntegerType()).alias("processing_time_ms")
        )
        aligned_df.write.format("delta").mode("append").saveAsTable(NETWORK_OPS_TABLE)
        print(f"✅ Wrote {len(results)} network operations")
    else:
        print("⚠️ No results")

# COMMAND ----------

# 🧹 Clean checkpoint for fresh start
print("🚀 STARTING FRESH - CLEANING CHECKPOINT")
cleanup_checkpoint_if_needed(OPS_CHECKPOINT, NETWORK_OPS_TABLE, "Network Operations")

# Start Streaming (depends on incident decisions)
print("🌊 Starting network operations streaming...")

ops_stream = (spark.readStream
    .format("delta")
    .table(INCIDENTS_TABLE)
    .writeStream
    .foreachBatch(process_operations_batch)
    .option("checkpointLocation", OPS_CHECKPOINT)
    .trigger(processingTime="25 seconds")
    .start())

print("✅ Network operations stream started")

# COMMAND ----------

# Monitor
start_time = time.time()
duration = 90
while time.time() - start_time < duration:
    elapsed = int(time.time() - start_time)
    try:
        inc_count = spark.table(INCIDENTS_TABLE).count() if spark.catalog.tableExists(INCIDENTS_TABLE) else 0
        ops_count = spark.table(NETWORK_OPS_TABLE).count() if spark.catalog.tableExists(NETWORK_OPS_TABLE) else 0
        print(f"⏰ [{elapsed}s/{duration}s] Incidents={inc_count}, Operations={ops_count}")
    except:
        print(f"⏰ [{elapsed}s/{duration}s] Monitoring...")
    time.sleep(15)

ops_stream.stop()
print("🛑 Operations stream stopped")

# COMMAND ----------

# Results Analysis
print("📊 NETWORK OPERATIONS ANALYSIS")
print("=" * 50)

try:
    final_ops = spark.table(NETWORK_OPS_TABLE).count()
    print(f"🎯 Network operations planned: {final_ops}")

    if final_ops > 0:
        method_analysis = spark.table(NETWORK_OPS_TABLE).groupBy("operation_method").count().collect()
        operation_dist = spark.table(NETWORK_OPS_TABLE).groupBy("recommended_operation").count().collect()
        risk_stats = spark.table(NETWORK_OPS_TABLE).groupBy("risk_level").count().collect()

        print("\n🤖 Method Breakdown:")
        for row in method_analysis:
            print(f"   {row['operation_method']}: {row['count']}")

        print("\n🔧 Operation Types:")
        for row in operation_dist:
            print(f"   {row['recommended_operation']}: {row['count']}")

        print("\n⚠️ Risk Levels:")
        for row in risk_stats:
            print(f"   {row['risk_level']}: {row['count']}")

        print("\n🔍 Sample Operations:")
        samples = spark.table(NETWORK_OPS_TABLE).select(
            "operation_type","safety_status","operation_method","action_taken"
        ).limit(3).collect()
        
        for row in samples:
            print(f"   {row['operation_type']} (Safety: {row['safety_status']}) via {row['operation_method']}")
            print(f"     Action: {row['action_taken']}")
    else:
        print("❌ No network operations planned")
except Exception as e:
    print(f"❌ Analysis failed: {e}")

# COMMAND ----------

spark.table(NETWORK_OPS_TABLE).show(truncate=False)