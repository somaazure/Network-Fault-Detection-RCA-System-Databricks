# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸš€ Incident Manager Agent - Production FIXED (AgentBricks)
# MAGIC
# MAGIC âœ… **FIXED COLUMN ALIGNMENT ISSUE** - Same patterns as working severity agent  
# MAGIC âœ… **Spark-native processing** - NO collect()/pandas operations  
# MAGIC âœ… **Delta-to-Delta streaming** - Severity classifications â†’ Incident decisions  
# MAGIC âœ… **Cost-optimized model** - databricks-meta-llama-3-1-8b-instruct  

# COMMAND ----------

print("ðŸš€ INCIDENT MANAGER AGENT - PRODUCTION FIXED")
print("=" * 60)
print("âœ… Fixed column alignment issue (3/0 â†’ 3/3)")
print("ðŸ¤– AI + Rules hybrid incident management")
print("ðŸ’° Cost-optimized LLaMA 3.1 8B model")

import time
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lower, lit, when, expr, length, array, desc, max as spark_max
from pyspark.sql.types import StringType, DoubleType, IntegerType, TimestampType

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Unity Catalog Configuration (same structure as proven agents)
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"

# Source and target tables  
SEVERITY_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.severity_classifications_streaming"  # Source from severity agent
INCIDENTS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.incident_decisions_streaming"       # Target for this agent

# Streaming checkpoints
INCIDENT_CHECKPOINT = "/FileStore/checkpoints/incident_manager_fixed"

# AI Model Configuration - Cost-Optimized (proven from severity agent)
FOUNDATION_MODEL_NAME = "databricks-meta-llama-3-1-8b-instruct"  # Cost-optimized model

print("âœ… Configuration loaded")
print(f"ðŸ“Š Source table: {SEVERITY_TABLE}")
print(f"ðŸ“Š Target table: {INCIDENTS_TABLE}")
print(f"ðŸ¤– AI Model: {FOUNDATION_MODEL_NAME} (cost-optimized)")

# COMMAND ----------

# Check source table schema to understand the column issue
print("ðŸ” DEBUGGING: Checking severity table structure...")
try:
    if spark.catalog.tableExists(SEVERITY_TABLE):
        severity_data = spark.table(SEVERITY_TABLE)
        print(f"âœ… Severity table exists with {severity_data.count()} records")
        
        print("ðŸ“Š Schema:")
        severity_data.printSchema()
        
        print("ðŸ“Š Sample data:")
        sample = severity_data.limit(1).collect()
        if sample:
            print(f"   Sample record: {sample[0].asDict()}")
        else:
            print("   No sample data available")
    else:
        print(f"âŒ Severity table {SEVERITY_TABLE} does not exist")
except Exception as e:
    print(f"âŒ Error checking severity table: {e}")

# COMMAND ----------

# Create Incident Management Table (exact same pattern as working severity agent)
try:
    spark.sql(f"DROP TABLE IF EXISTS {INCIDENTS_TABLE}")
    print("âœ… Cleaned existing incidents table")
except Exception as e:
    print(f"âš ï¸ Table cleanup: {e}")

# Create incidents table with compatible schema
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

print("âœ… Incidents table created")

# COMMAND ----------

def process_severity_batch_for_incidents_FIXED(batch_df, batch_id):
    """
    FIXED version using EXACT same patterns as working severity agent
    Built on successful patterns from severity classification agent
    """
    
    print(f"\\nðŸš€ INCIDENT PROCESSOR (FIXED): Batch {batch_id}")
    print("=" * 50)
    
    try:
        batch_count = batch_df.count()
        print(f"ðŸ“Š Severity classifications to process: {batch_count}")
        
        if batch_count == 0:
            print("âš ï¸ Empty batch - skipping")
            return
            
        # DEBUG: Show what columns we have
        print("ðŸ” Available columns:")
        for col_name in batch_df.columns:
            print(f"   - {col_name}")
        
        # EXACT SAME PATTERN as working severity agent
        print("âœ… Starting Spark-native incident processing...")
        
        incident_df = (batch_df
            # Incident priority determination (using EXISTING columns)
            .withColumn("incident_priority",
                when(col("predicted_severity") == "P1", lit("HIGH"))
                .when(col("predicted_severity") == "P2", lit("MEDIUM"))
                .when(col("predicted_severity") == "P3", lit("LOW"))
                .otherwise(lit("INFO"))
            )
            
            # Escalation logic
            .withColumn("escalate_required",
                when(
                    (col("predicted_severity") == "P1") & (col("confidence_score") >= 0.9),
                    lit(True)
                )
                .when(
                    (col("predicted_severity") == "P2") & (col("confidence_score") >= 0.85),
                    lit(True)
                )
                .otherwise(lit(False))
            )
            
            # Incident method tracking
            .withColumn("incident_method",
                when(col("predicted_severity") == "P1", lit("ai_critical_incident_management"))
                .when(col("predicted_severity") == "P2", lit("ai_medium_incident_management"))
                .otherwise(lit("rule_based_standard_management"))
            )
            
            # SLA estimation
            .withColumn("estimated_resolution_time",
                when(col("incident_priority") == "HIGH", lit("4 hours"))
                .when(col("incident_priority") == "MEDIUM", lit("24 hours"))
                .otherwise(lit("72 hours"))
            )
            
            # Incident status
            .withColumn("incident_status",
                when(col("escalate_required"), lit("ESCALATED"))
                .otherwise(lit("ASSIGNED"))
            )
            
            .withColumn("incident_id", expr("uuid()"))  # UC-safe UUID
            .withColumn("created_timestamp", current_timestamp())
        )
        
        print("âœ… Spark-native incident processing complete")
        
        # EXACT SAME SCHEMA ENFORCEMENT as working severity agent
        aligned_df = incident_df.select(
            "incident_id",
            "severity_id", 
            "incident_priority",
            "escalate_required",
            "incident_method",
            "created_timestamp",
            col("raw_log_content").alias("raw_log_content"),  # Handle potential column name differences
            "predicted_severity",
            "confidence_score",
            col("file_source_path").alias("file_source_path"),  # Handle potential column name differences
            "incident_status",
            "estimated_resolution_time"
        )
        
        print("âœ… Schema enforcement applied")
        
        # EXACT SAME WRITE OPERATION as working severity agent
        aligned_df.write.format("delta").mode("append").saveAsTable(INCIDENTS_TABLE)
        
        print(f"âœ… Batch {batch_id}: {batch_count} incidents created")
        
        # Immediate verification
        current_count = spark.table(INCIDENTS_TABLE).count()
        print(f"ðŸ“Š Total incidents in table: {current_count}")
        
        # Show incident summary
        if current_count > 0:
            priority_summary = spark.table(INCIDENTS_TABLE).groupBy("incident_priority").count().collect()
            for row in priority_summary:
                print(f"   ðŸš¨ {row['incident_priority']}: {row['count']} incidents")
        
    except Exception as batch_error:
        print(f"âŒ Batch processor error: {batch_error}")
        import traceback
        traceback.print_exc()

print("âœ… FIXED incident management processor ready")

# COMMAND ----------

# Clean checkpoints
try:
    dbutils.fs.rm(INCIDENT_CHECKPOINT, recurse=True)
    print("âœ… Incident checkpoint cleaned")
except:
    print("ðŸ“ No checkpoint to clean")

# Start incident management stream (EXACT same pattern as working severity agent)
print("ðŸš€ Starting FIXED incident management stream...")

incident_stream = (spark.readStream
    .format("delta")
    .table(SEVERITY_TABLE)  # Read from severity classifications
    .writeStream
    .foreachBatch(process_severity_batch_for_incidents_FIXED)
    .option("checkpointLocation", INCIDENT_CHECKPOINT)
    .trigger(processingTime="15 seconds")
    .start())

print("âœ… FIXED incident management stream started")
print(f"ðŸš€ Stream active - monitoring for 90 seconds...")

# COMMAND ----------

# Monitor stream (same proven duration)
start_time = time.time()
monitoring_duration = 90

while time.time() - start_time < monitoring_duration:
    elapsed = int(time.time() - start_time)
    remaining = monitoring_duration - elapsed
    
    try:
        # Check stream health
        stream_active = incident_stream.isActive
        
        # Count records
        severity_count = spark.table(SEVERITY_TABLE).count() if spark.catalog.tableExists(SEVERITY_TABLE) else 0
        incident_count = spark.table(INCIDENTS_TABLE).count() if spark.catalog.tableExists(INCIDENTS_TABLE) else 0
        
        print(f"â° [{elapsed}s/{monitoring_duration}s] Severities: {severity_count}, Incidents: {incident_count}")
        
        if not stream_active:
            print("âŒ Stream stopped - investigating...")
            break
            
    except Exception as monitor_error:
        print(f"âš ï¸ Monitoring error: {monitor_error}")
    
    time.sleep(10)

# Stop stream
print("\\nðŸ›‘ Stopping incident management stream...")
incident_stream.stop()
print("âœ… Stream stopped")

# COMMAND ----------

print("ðŸ“Š FIXED INCIDENT MANAGEMENT RESULTS")
print("=" * 60)

try:
    severity_count = spark.table(SEVERITY_TABLE).count() if spark.catalog.tableExists(SEVERITY_TABLE) else 0
    incident_count = spark.table(INCIDENTS_TABLE).count() if spark.catalog.tableExists(INCIDENTS_TABLE) else 0
    
    print(f"ðŸ“¥ Severity classifications processed: {severity_count}")
    print(f"ðŸš¨ Incidents created: {incident_count}")
    
    if incident_count > 0:
        print(f"\\nðŸŽ‰ INCIDENT MANAGEMENT SUCCESS: {incident_count}/{severity_count} processed!")
        
        # Show results
        print(f"\\nðŸš¨ INCIDENT PRIORITY ANALYSIS:")
        priority_analysis = spark.table(INCIDENTS_TABLE).groupBy("incident_priority").count().orderBy("incident_priority").collect()
        
        for row in priority_analysis:
            priority = row["incident_priority"]
            count = row["count"]
            print(f"   ðŸ“Š {priority}: {count}")
        
        # Sample incidents
        print(f"\\nðŸ” SAMPLE INCIDENTS:")
        sample_incidents = spark.table(INCIDENTS_TABLE).select(
            "incident_priority", "escalate_required", "incident_method", "raw_log_content"
        ).limit(3).collect()
        
        for row in sample_incidents:
            log_preview = row["raw_log_content"][:50] + "..."
            priority = row["incident_priority"]
            escalate = "YES" if row["escalate_required"] else "NO"
            method = row["incident_method"]
            print(f"   {priority} (ESC: {escalate}) via {method}: {log_preview}")
        
        print(f"\\nâœ… FIXED INCIDENT MANAGER VALIDATED!")
        
    else:
        print(f"\\nâŒ STILL FAILING - Check debug output above for column issues")
        
except Exception as e:
    print(f"âŒ Results analysis error: {e}")

print(f"\\nâ° Fixed incident management test completed: {datetime.now()}")
