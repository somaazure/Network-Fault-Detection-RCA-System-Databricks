# Databricks notebook source
# MAGIC %md
# MAGIC # 01-Data-Ingestion
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
# MAGIC # Network Log Data Ingestion Pipeline
# MAGIC 
# MAGIC This notebook replaces the Kafka-based ingestion system with Databricks Structured Streaming.
# MAGIC It processes network log files and ingests them into Delta tables for further analysis.

# COMMAND ----------

# MAGIC %pip install databricks-sdk

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import re

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NetworkLogIngestion") \
    .getOrCreate()

# Set up Unity Catalog
spark.sql("USE CATALOG network_fault_detection")
spark.sql("USE SCHEMA raw_data")

print("✅ Spark session initialized with Unity Catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Log Parsing Schema

# COMMAND ----------

# Define schema for network logs
network_log_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("log_level", StringType(), True),
    StructField("node_id", StringType(), True),
    StructField("message", StringType(), True),
    StructField("raw_content", StringType(), True)
])

def parse_network_log(log_line):
    """
    Parse network log line into structured format
    Expected format: [YYYY-MM-DD HH:MM:SS] LEVEL Node-ID: Message
    """
    if not log_line or log_line.strip() == "":
        return None
    
    # Regex pattern for log parsing
    pattern = r'\[([^\]]+)\]\s+(\w+)\s+([^:]+):\s*(.*)'
    match = re.match(pattern, log_line.strip())
    
    if match:
        timestamp_str, log_level, node_id, message = match.groups()
        
        # Parse timestamp
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except:
            timestamp = None
            
        return {
            "timestamp": timestamp,
            "log_level": log_level,
            "node_id": node_id.strip(),
            "message": message.strip(),
            "raw_content": log_line
        }
    else:
        # Return raw line if parsing fails
        return {
            "timestamp": None,
            "log_level": "UNKNOWN",
            "node_id": "UNKNOWN",
            "message": log_line,
            "raw_content": log_line
        }

# Register UDF
parse_log_udf = udf(parse_network_log, network_log_schema)

print("✅ Log parsing schema and UDF defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Ingestion from Legacy Log Files

# COMMAND ----------

def ingest_legacy_log_files(logs_path="/workspace/logs"):
    """
    Ingest existing log files from the legacy logs/ directory
    """
    try:
        # Read text files from logs directory
        raw_logs_df = spark.read.text(f"{logs_path}/*.txt")
        
        if raw_logs_df.count() == 0:
            print("⚠️ No log files found in the specified directory")
            return
        
        # Add source file information
        raw_logs_df = raw_logs_df.withColumn(
            "source_file", 
            input_file_name()
        ).withColumn(
            "ingestion_time",
            current_timestamp()
        )
        
        # Parse log lines
        parsed_logs_df = raw_logs_df.select(
            col("source_file"),
            parse_log_udf(col("value")).alias("parsed_log"),
            col("ingestion_time")
        ).select(
            col("source_file"),
            col("parsed_log.timestamp").alias("timestamp"),
            col("parsed_log.log_level").alias("log_level"), 
            col("parsed_log.node_id").alias("node_id"),
            col("parsed_log.message").alias("message"),
            col("parsed_log.raw_content").alias("raw_content"),
            col("ingestion_time")
        ).filter(
            col("raw_content").isNotNull() & 
            (col("raw_content") != "")
        )
        
        # Write to Delta table
        parsed_logs_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("network_fault_detection.raw_data.network_logs")
        
        record_count = parsed_logs_df.count()
        print(f"✅ Successfully ingested {record_count} log records")
        
        # Show sample data
        print("Sample ingested data:")
        parsed_logs_df.show(5, truncate=False)
        
    except Exception as e:
        print(f"❌ Error during batch ingestion: {str(e)}")

# Execute batch ingestion
ingest_legacy_log_files()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Real-time Streaming Ingestion (replaces Kafka)

# COMMAND ----------

def setup_streaming_ingestion(source_path="/workspace/logs/stream", checkpoint_path="/workspace/checkpoints/logs"):
    """
    Set up streaming ingestion to monitor new log files
    This replaces the Kafka consumer functionality
    """
    
    # Create streaming DataFrame from file source
    streaming_logs_df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "text") \
        .option("cloudFiles.schemaLocation", checkpoint_path + "/schema") \
        .option("cloudFiles.includeExistingFiles", "true") \
        .load(source_path)
    
    # Add metadata and parse logs
    processed_streaming_df = streaming_logs_df.select(
        input_file_name().alias("source_file"),
        parse_log_udf(col("value")).alias("parsed_log"),
        current_timestamp().alias("ingestion_time")
    ).select(
        col("source_file"),
        col("parsed_log.timestamp").alias("timestamp"),
        col("parsed_log.log_level").alias("log_level"),
        col("parsed_log.node_id").alias("node_id"), 
        col("parsed_log.message").alias("message"),
        col("parsed_log.raw_content").alias("raw_content"),
        col("ingestion_time")
    ).filter(
        col("raw_content").isNotNull() & 
        (col("raw_content") != "")
    )
    
    # Write stream to Delta table
    query = processed_streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .table("network_fault_detection.raw_data.network_logs")
    
    print("✅ Streaming ingestion started")
    print(f"📁 Monitoring path: {source_path}")
    print(f"💾 Checkpoint location: {checkpoint_path}")
    
    return query

# Uncomment to start streaming (for production use)
# streaming_query = setup_streaming_ingestion()

print("ℹ️ Streaming ingestion setup complete (not started)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality and Validation

# COMMAND ----------

def validate_ingested_data():
    """
    Perform data quality checks on ingested network logs
    """
    
    network_logs_df = spark.table("network_fault_detection.raw_data.network_logs")
    
    print("📊 Data Quality Report")
    print("=" * 50)
    
    # Basic statistics
    total_records = network_logs_df.count()
    print(f"Total Records: {total_records:,}")
    
    if total_records > 0:
        # Records by source file
        print("\nRecords by Source File:")
        network_logs_df.groupBy("source_file") \
            .count() \
            .orderBy(desc("count")) \
            .show(truncate=False)
        
        # Records by log level
        print("Records by Log Level:")
        network_logs_df.groupBy("log_level") \
            .count() \
            .orderBy(desc("count")) \
            .show()
        
        # Records by node ID
        print("Top 10 Nodes by Log Volume:")
        network_logs_df.groupBy("node_id") \
            .count() \
            .orderBy(desc("count")) \
            .limit(10) \
            .show()
        
        # Timestamp range
        print("Timestamp Range:")
        network_logs_df.agg(
            min("timestamp").alias("earliest_timestamp"),
            max("timestamp").alias("latest_timestamp"),
            count("timestamp").alias("records_with_timestamp")
        ).show(truncate=False)
        
        # Data quality issues
        null_timestamps = network_logs_df.filter(col("timestamp").isNull()).count()
        unknown_nodes = network_logs_df.filter(col("node_id") == "UNKNOWN").count()
        
        print(f"\n⚠️ Data Quality Issues:")
        print(f"Records with null timestamps: {null_timestamps}")
        print(f"Records with unknown node IDs: {unknown_nodes}")
        
        if null_timestamps > 0 or unknown_nodes > 0:
            print("Consider improving log parsing logic for better data quality")
    
    print("\n✅ Data quality validation complete")

# Run validation
validate_ingested_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Configuration for Next Steps

# COMMAND ----------

# Store configuration for downstream notebooks
dbutils.widgets.text("logs_table", "network_fault_detection.raw_data.network_logs")
dbutils.widgets.text("ingestion_status", "completed")

print("✅ Data ingestion pipeline completed")
print("🔄 Ready for streaming pipeline and agent orchestration")
print("\nNext steps:")
print("1. Run notebook 02-streaming-pipeline.py for real-time processing")
print("2. Run notebook 03-agent-orchestration.py for AI agent analysis")
print("3. Access dashboard via notebook 04-dashboard.sql")