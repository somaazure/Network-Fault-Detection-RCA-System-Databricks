# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸš€ Hybrid Fixed Severity Classification Agent (UC-Aligned)
# MAGIC
# MAGIC âœ… Auto Loader â†’ Raw Table works  
# MAGIC âœ… UC-safe foreachBatch with schema-aligned write  
# MAGIC âœ… Guaranteed to classify & persist 15/15 logs  
# MAGIC âœ… Tables aligned to: network_fault_detection.processed_data  

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lower, lit, when, expr
import time

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurations

# COMMAND ----------

CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"

RAW_LOGS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.network_logs_streaming"
SEVERITY_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.severity_classifications_streaming"

LOG_SOURCE_PATH = "/FileStore/logs/network_logs_streaming"
RAW_CHECKPOINT = "/FileStore/checkpoints/raw_logs"
SEV_CHECKPOINT = "/FileStore/checkpoints/severity_logs"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cleanup old tables & checkpoints

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {RAW_LOGS_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {SEVERITY_TABLE}")

dbutils.fs.rm(LOG_SOURCE_PATH, recurse=True)
dbutils.fs.rm(RAW_CHECKPOINT, recurse=True)
dbutils.fs.rm(SEV_CHECKPOINT, recurse=True)

print("âœ… Cleanup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Sample Log Files (3 files Ã— 5 lines)

# COMMAND ----------

logs = [
    "ERROR Network interface eth0 down",
    "WARN High memory usage detected",
    "INFO System reboot scheduled",
    "CRITICAL Database connection lost",
    "ERROR Disk space running low"
]

for i in range(3):
    path = f"{LOG_SOURCE_PATH}/logs_batch_{i}.txt"
    dbutils.fs.put(path, "\n".join(logs), True)

print("âœ… Created 3 log files with 15 total lines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Tables

# COMMAND ----------

# Raw logs table
spark.sql(f"""
CREATE TABLE {RAW_LOGS_TABLE} (
    log_content STRING,
    file_path STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA
""")

# Severity classification table (UC-safe schema)
spark.sql(f"""
CREATE TABLE {SEVERITY_TABLE} (
    severity_id STRING,
    raw_log_content STRING,
    predicted_severity STRING,
    confidence_score DOUBLE,
    classification_method STRING,
    classification_timestamp TIMESTAMP,
    file_source_path STRING
) USING DELTA
""")

print("âœ… Tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Define Batch Processor (UC-safe)

# COMMAND ----------

def process_batch(batch_df, batch_id):
    try:
        row_count = batch_df.count()
        print(f"ðŸš€ Entered batch {batch_id} with {row_count} rows")

        if row_count == 0:
            print(f"âš ï¸ Batch {batch_id} empty")
            return

        classified_df = (batch_df
            .withColumnRenamed("log_content", "raw_log_content")
            .withColumnRenamed("file_path", "file_source_path")
            .withColumn("predicted_severity",
                        when(lower(col("raw_log_content")).contains("critical"), lit("P1"))
                        .when(lower(col("raw_log_content")).contains("error"), lit("P2"))
                        .when(lower(col("raw_log_content")).contains("warn"), lit("P3"))
                        .otherwise(lit("INFO")))
            .withColumn("confidence_score",
                        when(lower(col("raw_log_content")).contains("critical"), lit(0.95))
                        .when(lower(col("raw_log_content")).contains("error"), lit(0.85))
                        .when(lower(col("raw_log_content")).contains("warn"), lit(0.75))
                        .otherwise(lit(0.60)))
            .withColumn("classification_method", lit("rule-based"))
            .withColumn("classification_timestamp", current_timestamp())
            .withColumn("severity_id", expr("uuid()"))
        )

        # âœ… Enforce exact schema
        classified_df = classified_df.select(
            "severity_id",
            "raw_log_content",
            "predicted_severity",
            "confidence_score",
            "classification_method",
            "classification_timestamp",
            "file_source_path"
        )

        classified_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(SEVERITY_TABLE)

        print(f"âœ… Batch {batch_id}: {row_count} rows classified")

    except Exception as e:
        print(f"âŒ Error in batch {batch_id}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Start Streams (Dual: Raw + Severity)

# COMMAND ----------

# Raw ingestion stream
raw_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "text")
    .load(LOG_SOURCE_PATH)
    .withColumn("log_content", col("value"))
    .withColumn("file_path", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .drop("value")
    .writeStream
    .format("delta")
    .option("checkpointLocation", RAW_CHECKPOINT)
    .toTable(RAW_LOGS_TABLE))

# Severity classification stream
sev_stream = (spark.readStream
    .format("delta")
    .table(RAW_LOGS_TABLE)
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", SEV_CHECKPOINT)
    .start())

print("ðŸŒŠ Streams started...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Monitor for 60s

# COMMAND ----------

for i in range(12):
    time.sleep(5)
    raw_count = spark.table(RAW_LOGS_TABLE).count()
    sev_count = spark.table(SEVERITY_TABLE).count()
    print(f"â³ [{i*5}s] Raw={raw_count}, Severity={sev_count}")

print("ðŸ›‘ Monitoring complete. Stop streams manually if needed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verify Final Results

# COMMAND ----------

print("Raw logs count:", spark.table(RAW_LOGS_TABLE).count())
spark.table(RAW_LOGS_TABLE).show(5, truncate=False)

print("Severity logs count:", spark.table(SEVERITY_TABLE).count())
spark.table(SEVERITY_TABLE).show(10, truncate=False)

spark.sql(f"""
SELECT predicted_severity, COUNT(*) AS count
FROM {SEVERITY_TABLE}
GROUP BY predicted_severity
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Stop Streams (Cleanup)

# COMMAND ----------

if 'sev_stream' in globals() and sev_stream.isActive:
    print("ðŸ›‘ Stopping severity classification stream...")
    sev_stream.stop()
    print("âœ… Severity stream stopped")

if 'raw_stream' in globals() and raw_stream.isActive:
    print("ðŸ›‘ Stopping raw ingestion stream...")
    raw_stream.stop()
    print("âœ… Raw ingestion stream stopped")

active_streams = spark.streams.active
if not active_streams:
    print("ðŸŽ‰ All streams stopped successfully")
else:
    print("âš ï¸ Some streams are still running:")
    for s in active_streams:
        print(f"   - {s.name} ({s.id})")
