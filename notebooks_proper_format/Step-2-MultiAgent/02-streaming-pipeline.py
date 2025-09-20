# Databricks notebook source
# MAGIC %md
# MAGIC # 02-Streaming-Pipeline
# MAGIC
# MAGIC **Network Fault Detection RCA System**
# MAGIC
# MAGIC This notebook is part of the production-ready Network Fault Detection and Root Cause Analysis system.
# MAGIC
# MAGIC ## 🔧 Configuration
# MAGIC
# MAGIC ```python
# MAGIC # Secure configuration pattern
# MAGIC DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', dbutils.secrets.get('default', 'databricks-host'))
# MAGIC DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', dbutils.secrets.get('default', 'databricks-token'))
# MAGIC ```

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Network Incident Streaming Pipeline
# MAGIC 
# MAGIC This notebook processes ingested network logs in real-time to detect incidents and classify severity.
# MAGIC It replaces the manual log processing and triggers agent orchestration for RCA generation.

# COMMAND ----------

# MAGIC %pip install databricks-sdk

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import json

# Initialize Spark session  
spark = SparkSession.builder \
    .appName("NetworkIncidentStreaming") \
    .getOrCreate()

# Set up Unity Catalog
spark.sql("USE CATALOG network_fault_detection") 
spark.sql("USE SCHEMA processed_data")

print("✅ Spark session initialized for streaming pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incident Detection Logic

# COMMAND ----------

def classify_incident_severity(log_level, message, node_id):
    """
    Basic incident severity classification logic
    This will be enhanced by the AI agent in the next notebook
    """
    
    # Keywords for severity classification
    p1_keywords = ["outage", "down", "failed", "critical", "emergency", "fiber cut"]
    p2_keywords = ["degradation", "congestion", "performance", "timeout", "warning"]
    p3_keywords = ["info", "notice", "maintenance", "scheduled"]
    
    message_lower = message.lower()
    log_level_upper = log_level.upper()
    
    # P1: Critical issues
    if (log_level_upper in ["ERROR", "FATAL", "CRITICAL"] or 
        any(keyword in message_lower for keyword in p1_keywords)):
        return "P1"
    
    # P2: Major issues  
    elif (log_level_upper in ["WARN", "WARNING"] or
          any(keyword in message_lower for keyword in p2_keywords)):
        return "P2"
        
    # P3: Minor issues
    else:
        return "P3"

def determine_incident_type(message, log_level):
    """
    Categorize the type of network incident
    """
    message_lower = message.lower()
    
    if any(word in message_lower for word in ["outage", "down", "offline"]):
        return "outage"
    elif any(word in message_lower for word in ["congestion", "overload", "capacity"]):
        return "congestion"  
    elif any(word in message_lower for word in ["performance", "latency", "delay"]):
        return "performance_degradation"
    elif any(word in message_lower for word in ["timeout", "connection", "unreachable"]):
        return "connectivity_issue"
    else:
        return "unknown"

def determine_impact_scope(affected_nodes_count):
    """
    Determine the scope of impact based on affected nodes
    """
    if affected_nodes_count >= 10:
        return "regional"
    elif affected_nodes_count >= 3:
        return "multiple_nodes"
    else:
        return "single_node"

# Register UDFs
classify_severity_udf = udf(classify_incident_severity, StringType())
determine_type_udf = udf(determine_incident_type, StringType())
determine_scope_udf = udf(determine_impact_scope, StringType())

print("✅ Incident detection UDFs registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Incident Detection

# COMMAND ----------

def setup_incident_detection_stream():
    """
    Set up streaming pipeline to detect incidents from raw network logs
    """
    
    # Read streaming data from network logs table
    network_logs_stream = spark.readStream \
        .format("delta") \
        .table("network_fault_detection.raw_data.network_logs")
    
    # Filter for significant log events (not INFO level)
    significant_logs = network_logs_stream.filter(
        col("log_level").isin(["ERROR", "WARN", "WARNING", "FATAL", "CRITICAL", "ALERT"])
    )
    
    # Add incident classification
    incident_logs = significant_logs.select(
        col("log_id"),
        col("source_file"),
        col("timestamp").alias("detected_timestamp"),
        classify_severity_udf(col("log_level"), col("message"), col("node_id")).alias("severity_level"),
        determine_type_udf(col("message"), col("log_level")).alias("incident_type"),
        array(col("node_id")).alias("affected_nodes"),
        lit("single_node").alias("impact_scope"),
        lit("open").alias("status"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
        col("log_level"),
        col("node_id"),
        col("message"),
        col("raw_content")
    )
    
    return incident_logs

print("✅ Incident detection stream configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incident Aggregation and Deduplication

# COMMAND ----------

def setup_incident_aggregation():
    """
    Aggregate related log entries into single incidents to avoid duplicates
    """
    
    incident_logs = setup_incident_detection_stream()
    
    # Window for aggregating incidents (5 minute windows)
    windowed_incidents = incident_logs \
        .withWatermark("detected_timestamp", "10 minutes") \
        .groupBy(
            window(col("detected_timestamp"), "5 minutes"),
            col("node_id"),
            col("severity_level"),
            col("incident_type")
        ).agg(
            first("detected_timestamp").alias("detected_timestamp"),
            collect_set("log_id").alias("source_log_ids"), 
            first("severity_level").alias("severity_level"),
            first("incident_type").alias("incident_type"),
            collect_set("node_id").alias("affected_nodes"),
            first("impact_scope").alias("impact_scope"),
            first("status").alias("status"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
            collect_list("message").alias("all_messages"),
            collect_list("raw_content").alias("all_raw_content")
        ).select(
            col("source_log_ids")[0].alias("source_log_id"),  # Primary log ID
            col("detected_timestamp"),
            col("severity_level"),
            col("incident_type"), 
            col("affected_nodes"),
            determine_scope_udf(size(col("affected_nodes"))).alias("impact_scope"),
            col("status"),
            col("created_at"),
            col("updated_at")
        )
    
    return windowed_incidents

print("✅ Incident aggregation configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Incidents to Delta Table

# COMMAND ----------

def start_incident_processing():
    """
    Start the streaming pipeline to process incidents
    """
    
    aggregated_incidents = setup_incident_aggregation()
    
    # Write to incidents table
    incidents_query = aggregated_incidents.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/workspace/checkpoints/incidents") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="30 seconds") \
        .table("network_fault_detection.processed_data.incidents")
    
    print("✅ Incident processing stream started")
    print("📊 Writing incidents to: network_fault_detection.processed_data.incidents")
    print("⏱️ Processing interval: 30 seconds")
    
    return incidents_query

# Uncomment to start the streaming pipeline
# incident_stream = start_incident_processing()

print("ℹ️ Incident processing pipeline configured (not started)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Orchestration Trigger

# COMMAND ----------

def setup_agent_trigger_stream():
    """
    Set up a stream that triggers AI agent processing for new incidents
    This replaces the manual agent orchestration trigger
    """
    
    # Read incidents stream
    incidents_stream = spark.readStream \
        .format("delta") \
        .table("network_fault_detection.processed_data.incidents")
    
    # Filter for new P1 and P2 incidents that need RCA
    high_priority_incidents = incidents_stream.filter(
        col("severity_level").isin(["P1", "P2"]) &
        col("status").isin(["open"])
    )
    
    # Add agent processing metadata
    agent_trigger_data = high_priority_incidents.select(
        col("incident_id"),
        col("source_log_id"),
        col("severity_level"),
        col("incident_type"),
        col("detected_timestamp"),
        lit("pending").alias("agent_status"),
        current_timestamp().alias("trigger_time")
    )
    
    # Write to a trigger table for agent orchestration
    trigger_query = agent_trigger_data.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/workspace/checkpoints/agent_triggers") \
        .trigger(processingTime="1 minute") \
        .table("network_fault_detection.operations.agent_triggers")
    
    print("✅ Agent trigger stream configured")
    print("🤖 High-priority incidents will trigger AI agent processing")
    
    return trigger_query

# Create the agent triggers table if it doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS network_fault_detection.operations.agent_triggers (
    trigger_id STRING GENERATED ALWAYS AS IDENTITY,
    incident_id STRING,
    source_log_id STRING,
    severity_level STRING,
    incident_type STRING,
    detected_timestamp TIMESTAMP,
    agent_status STRING DEFAULT 'pending',
    trigger_time TIMESTAMP,
    processed_time TIMESTAMP,
    
    CONSTRAINT valid_agent_status CHECK (agent_status IN ('pending', 'processing', 'completed', 'failed'))
) USING DELTA
""")

print("✅ Agent triggers table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Metrics

# COMMAND ----------

def display_streaming_metrics():
    """
    Display real-time metrics for the streaming pipeline
    """
    
    print("📊 Streaming Pipeline Metrics")
    print("=" * 50)
    
    # Recent incidents summary
    recent_incidents = spark.sql("""
        SELECT 
            severity_level,
            incident_type,
            COUNT(*) as count,
            MAX(detected_timestamp) as latest_incident
        FROM network_fault_detection.processed_data.incidents 
        WHERE detected_timestamp >= current_timestamp() - INTERVAL 1 HOUR
        GROUP BY severity_level, incident_type
        ORDER BY severity_level, count DESC
    """)
    
    if recent_incidents.count() > 0:
        print("Recent Incidents (Last Hour):")
        recent_incidents.show(truncate=False)
    else:
        print("No incidents in the last hour")
    
    # Agent trigger backlog
    agent_backlog = spark.sql("""
        SELECT 
            agent_status,
            COUNT(*) as count
        FROM network_fault_detection.operations.agent_triggers
        WHERE trigger_time >= current_timestamp() - INTERVAL 6 HOURS
        GROUP BY agent_status
        ORDER BY count DESC
    """)
    
    if agent_backlog.count() > 0:
        print("\nAgent Processing Backlog:")
        agent_backlog.show()
    else:
        print("No agent triggers in the last 6 hours")

# Run metrics display
display_streaming_metrics()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Control and Configuration

# COMMAND ----------

# Configuration widgets for runtime control
dbutils.widgets.dropdown("enable_incident_stream", "false", ["true", "false"])
dbutils.widgets.dropdown("enable_agent_triggers", "false", ["true", "false"])
dbutils.widgets.text("processing_interval", "30")

enable_incidents = dbutils.widgets.get("enable_incident_stream") == "true"
enable_agents = dbutils.widgets.get("enable_agent_triggers") == "true"
processing_interval = dbutils.widgets.get("processing_interval")

print(f"Configuration:")
print(f"- Enable Incident Stream: {enable_incidents}")
print(f"- Enable Agent Triggers: {enable_agents}")
print(f"- Processing Interval: {processing_interval} seconds")

if enable_incidents:
    print("\n🚀 Starting incident processing stream...")
    # incident_stream = start_incident_processing()

if enable_agents:
    print("\n🤖 Starting agent trigger stream...")  
    # agent_stream = setup_agent_trigger_stream()

print("\n✅ Streaming pipeline configuration complete")
print("🔄 Ready for agent orchestration (notebook 03-agent-orchestration.py)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Enable Streaming**: Set widgets to `true` and uncomment stream starts
# MAGIC 2. **Monitor Pipeline**: Use the metrics section to monitor performance
# MAGIC 3. **Agent Processing**: Run notebook `03-agent-orchestration.py` to process incidents
# MAGIC 4. **Dashboard**: Access real-time dashboard via `04-dashboard.sql`