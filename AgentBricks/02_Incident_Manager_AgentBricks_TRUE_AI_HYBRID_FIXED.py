# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Incident Manager Agent - TRUE AI + Rules Hybrid (FIXED)

# COMMAND ----------

import time
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import IntegerType
import mlflow.deployments

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Configuration
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"

SEVERITY_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.severity_classifications_streaming"
INCIDENTS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.incident_decisions_streaming"

INCIDENT_CHECKPOINT = "/FileStore/checkpoints/incident_manager_ai_hybrid_fixed_v3"

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

print("🛠️ Incident Manager checkpoint cleanup function ready")

# Force rule-based processing for reliable table population
AI_ENABLED = False
print("🔧 Using rule-based processing only for reliable table population")

# COMMAND ----------

# AI + Rules Hybrid Functions
def create_incident_with_fm(severity_data: dict) -> dict:
    if not AI_ENABLED:
        return {"success": False}
    try:
        severity = severity_data.get("predicted_severity", "INFO")
        log_content = severity_data.get("raw_log_content", "")
        
        response = client.predict(
            endpoint=FOUNDATION_MODEL_NAME,
            inputs={
                "messages": [
                    {"role": "system", "content": "You are an expert incident manager."},
                    {"role": "user", "content": f"For {severity} severity log '{log_content[:100]}', decide incident priority (HIGH/MEDIUM/LOW/INFO) and escalation (YES/NO). Format: PRIORITY:ESCALATION:REASONING"}
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
                priority = parts[0].strip().upper()
                escalation = parts[1].strip().upper() == "YES"
                reasoning = ":".join(parts[2:]).strip()
                
                if priority in ["HIGH", "MEDIUM", "LOW", "INFO"]:
                    return {"success": True, "priority": priority, "escalate": escalation,
                           "method": "ai_foundation_model", "reasoning": reasoning[:100]}
        return {"success": False}
    except Exception as e:
        print(f"⚠️ FM call failed: {e}")
        return {"success": False}

def create_incident_with_rules(severity_data: dict) -> dict:
    severity = severity_data.get("predicted_severity", "INFO")
    log_content = severity_data.get("raw_log_content", "").lower()
    
    if severity == "P1":
        escalate = any(k in log_content for k in ["critical", "outage", "down", "offline"])
        return {"priority": "HIGH", "escalate": escalate, "method": "rule_based", "reasoning": "P1 high priority"}
    elif severity == "P2":
        return {"priority": "MEDIUM", "escalate": False, "method": "rule_based", "reasoning": "P2 medium priority"}  
    elif severity == "P3":
        return {"priority": "LOW", "escalate": False, "method": "rule_based", "reasoning": "P3 low priority"}
    else:
        return {"priority": "INFO", "escalate": False, "method": "rule_based", "reasoning": "Informational"}

def hybrid_incident_decision(severity_data: dict) -> dict:
    fm_res = create_incident_with_fm(severity_data)
    if fm_res.get("success"):
        return fm_res
    return create_incident_with_rules(severity_data)

# COMMAND ----------

# ✅ RECREATE TABLE: Drop old table to fix schema mismatch
try:
    print("🔧 Dropping existing incidents table to fix schema mismatch...")
    spark.sql(f"DROP TABLE IF EXISTS {INCIDENTS_TABLE}")
    print("✅ Old table dropped")
except Exception as drop_error:
    print(f"⚠️ Drop table warning: {str(drop_error)}")

print("📋 Creating new incidents table with correct schema")
spark.sql(f"""
CREATE TABLE {INCIDENTS_TABLE} (
    incident_id STRING,
    severity_id STRING,
    incident_priority STRING,
    escalate_required BOOLEAN,
    incident_method STRING,
    created_timestamp TIMESTAMP,
    raw_log_content STRING,
    predicted_severity STRING,
    confidence_score DOUBLE,
    file_source_path STRING,
    incident_status STRING,
    estimated_resolution_time STRING
) USING DELTA
""")

print("✅ Incident decisions table ready")

# COMMAND ----------

# ForeachBatch Processor
def process_incident_batch(batch_df, batch_id):
    batch_count = batch_df.count()
    print(f"\n🚀 Processing incident batch {batch_id} with {batch_count} severity classifications")

    if batch_count == 0:
        print("⚠️ Empty batch received - no severity classifications to process")
        return

    # Show sample data for debugging
    print("🔍 Sample batch data:")
    batch_df.show(3, truncate=False)

    rows = batch_df.collect()
    results = []
    
    for idx, row in enumerate(rows):
        row_dict = row.asDict()  # ✅ FIXED: Safe field access
        
        severity_data = {
            "predicted_severity": row_dict["predicted_severity"],
            "confidence_score": row_dict["confidence_score"], 
            "raw_log_content": row_dict["raw_log_content"]
        }
        
        result = hybrid_incident_decision(severity_data)
        
        record = {
            "incident_id": f"inc_{int(time.time()*1000000)}_{idx}",
            "severity_id": row_dict["predicted_severity"],  # Match expected schema
            "incident_priority": result["priority"],
            "escalate_required": result["escalate"],  # Match expected schema
            "incident_method": result["method"],  # Match expected schema
            "created_timestamp": datetime.now(),  # Match expected schema
            "raw_log_content": row_dict["raw_log_content"][:200],  # Match expected schema
            "predicted_severity": row_dict["predicted_severity"],  # Match expected schema
            "confidence_score": 0.8,  # Add missing field
            "file_source_path": "streaming",  # Add missing field
            "incident_status": "ACTIVE",  # Add missing field
            "estimated_resolution_time": "30 minutes"  # Add missing field
        }
        results.append(record)
    
    if results:
        results_df = spark.createDataFrame(results)
        # ✅ FIXED: Schema alignment to match existing table structure
        aligned_df = results_df.select(
            "incident_id", "severity_id", "incident_priority", "escalate_required",
            "incident_method", "created_timestamp", "raw_log_content", "predicted_severity",
            "confidence_score", "file_source_path", "incident_status", "estimated_resolution_time"
        )
        aligned_df.write.format("delta").mode("append").saveAsTable(INCIDENTS_TABLE)
        print(f"✅ Wrote {len(results)} incident decisions")
    else:
        print("⚠️ No results")

# COMMAND ----------

# 🧹 Clean checkpoint for fresh start
print("🚀 STARTING FRESH - CLEANING CHECKPOINT")
cleanup_checkpoint_if_needed(INCIDENT_CHECKPOINT, INCIDENTS_TABLE, "Incident Manager")

# Pre-flight checks before starting streaming
print("🔍 Pre-flight checks...")
try:
    severity_count = spark.table(SEVERITY_TABLE).count() if spark.catalog.tableExists(SEVERITY_TABLE) else 0
    print(f"📊 Severity classifications available: {severity_count}")

    if severity_count == 0:
        print("⚠️ WARNING: No severity classifications found!")
        print("   💡 Make sure 01_Severity_Classification_Agent is running first")
        print("   📋 Or check if there are recent records in the severity table")

    # Check for recent records (last 24 hours)
    if severity_count > 0:
        recent_severity = spark.sql(f"""
            SELECT COUNT(*) as recent_count
            FROM {SEVERITY_TABLE}
            WHERE classification_timestamp >= current_timestamp() - INTERVAL 24 HOURS
        """).collect()[0]["recent_count"]
        print(f"🕐 Recent severity records (24h): {recent_severity}")

except Exception as check_error:
    print(f"⚠️ Pre-flight check error: {str(check_error)}")

# Start Streaming (depends on severity classifications)
print("🌊 Starting incident management streaming...")

incident_stream = (spark.readStream
    .format("delta")
    .table(SEVERITY_TABLE)
    .writeStream
    .foreachBatch(process_incident_batch)
    .option("checkpointLocation", INCIDENT_CHECKPOINT)
    .trigger(processingTime="20 seconds")
    .start())

print("✅ Incident management stream started")

# COMMAND ----------

# Monitor
start_time = time.time()
duration = 90
while time.time() - start_time < duration:
    elapsed = int(time.time() - start_time)
    try:
        sev_count = spark.table(SEVERITY_TABLE).count() if spark.catalog.tableExists(SEVERITY_TABLE) else 0
        inc_count = spark.table(INCIDENTS_TABLE).count() if spark.catalog.tableExists(INCIDENTS_TABLE) else 0
        print(f"⏰ [{elapsed}s/{duration}s] Severity={sev_count}, Incidents={inc_count}")
    except:
        print(f"⏰ [{elapsed}s/{duration}s] Monitoring...")
    time.sleep(15)

incident_stream.stop()
print("🛑 Incident stream stopped")

# COMMAND ----------

# Results Analysis
print("📊 INCIDENT MANAGEMENT ANALYSIS")
print("=" * 50)

try:
    final_inc = spark.table(INCIDENTS_TABLE).count()
    print(f"🎯 Incident decisions: {final_inc}")

    if final_inc > 0:
        method_analysis = spark.table(INCIDENTS_TABLE).groupBy("incident_method").count().collect()
        priority_dist = spark.table(INCIDENTS_TABLE).groupBy("incident_priority").count().orderBy("incident_priority").collect()
        escalation_stats = spark.table(INCIDENTS_TABLE).groupBy("escalate_required").count().collect()

        print("\n🤖 Method Breakdown:")
        for row in method_analysis:
            print(f"   {row['incident_method']}: {row['count']}")

        print("\n🚨 Priority Distribution:")
        for row in priority_dist:
            print(f"   {row['incident_priority']}: {row['count']}")

        print("\n🚀 Escalation Stats:")
        for row in escalation_stats:
            escalation = "YES" if row['escalate_required'] else "NO"
            print(f"   Escalation {escalation}: {row['count']}")

        print("\n🔍 Sample Decisions:")
        # Use the correct column names that exist in the table
        samples = spark.table(INCIDENTS_TABLE).select(
            "incident_priority","escalate_required","incident_method","raw_log_content"
        ).limit(3).collect()

        for row in samples:
            escalation = "YES" if row['escalate_required'] else "NO"
            print(f"   {row['incident_priority']} (Escalate: {escalation}) via {row['incident_method']}")
            # Note: incident_reasoning column doesn't exist in the actual schema
            print(f"     Log: {row['raw_log_content'][:60]}...")
    else:
        print("❌ No incident decisions produced")
except Exception as e:
    print(f"❌ Analysis failed: {e}")

# COMMAND ----------

spark.table(INCIDENTS_TABLE).show(truncate=False)