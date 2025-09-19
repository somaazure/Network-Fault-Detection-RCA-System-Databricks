# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸš€ Severity Classification - AI + Rules Hybrid (Driver-Side FM Calls)

# COMMAND ----------

import time
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import IntegerType
import mlflow.deployments

spark = SparkSession.builder.getOrCreate()

print("=" * 80)
print("ðŸ”§ RUNNING: AI + Rules Hybrid (Driver-side FM Calls)")
print("ðŸ¤– This version ensures FM runs on driver to avoid worker failures")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"

RAW_LOGS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.network_logs_streaming"
SEVERITY_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.severity_classifications_streaming"

LOG_SOURCE_PATH = "/FileStore/logs/network_logs_streaming"
RAW_CHECKPOINT = "/FileStore/checkpoints/raw_ai_rules_raw_driver"
SEV_CHECKPOINT = "/FileStore/checkpoints/raw_ai_rules_sev_driver"

FOUNDATION_MODEL_NAME = "databricks-meta-llama-3-1-8b-instruct"

print("ðŸ”§ Initializing Foundation Model client...")
try:
    client = mlflow.deployments.get_deploy_client("databricks")
    AI_ENABLED = True
    print(f"âœ… AI model enabled: {FOUNDATION_MODEL_NAME}")
except Exception as e:
    client = None
    AI_ENABLED = False
    print(f"âš ï¸ FM client init failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Classification Functions

# COMMAND ----------

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
    """Force FM calls on driver, not Spark workers"""
    if not AI_ENABLED:
        return {"success": False}
    try:
        response = client.predict(
            endpoint=FOUNDATION_MODEL_NAME,
            inputs={
                "messages": [
                    {"role": "system", "content": "You are an expert incident classifier."},
                    {"role": "user", "content": f"Classify this log as P1, P2, P3 or INFO: {log_content}"}
                ],
                "temperature": 0.1,
                "max_tokens": 20
            }
        )
        prediction = response["choices"][0]["message"]["content"].strip().upper()
        severity = "INFO"
        if "P1" in prediction: severity = "P1"
        elif "P2" in prediction: severity = "P2"
        elif "P3" in prediction: severity = "P3"
        elif "INFO" in prediction: severity = "INFO"
        return {
            "success": True,
            "severity": severity,
            "method": "ai_foundation_model",
            "confidence": 0.85,
            "reasoning": f"FM classified {severity}"
        }
    except Exception as e:
        print(f"âŒ FM error: {e}")
        return {"success": False}

def hybrid_classify(log_content: str) -> dict:
    fm_res = classify_with_fm(log_content)
    if fm_res.get("success"):
        return fm_res
    else:
        return classify_with_rules(log_content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Setup Tables + Sample Logs

# COMMAND ----------

# Create raw logs table if missing
try:
    spark.table(RAW_LOGS_TABLE).count()
except:
    spark.sql(f"""
    CREATE TABLE {RAW_LOGS_TABLE} (
        log_content STRING,
        file_path STRING,
        ingestion_timestamp TIMESTAMP
    ) USING DELTA
    """)

# Create severity table if missing
try:
    spark.table(SEVERITY_TABLE).count()
except:
    spark.sql(f"""
    CREATE TABLE {SEVERITY_TABLE} (
        severity_id STRING,
        raw_log_content STRING,
        predicted_severity STRING,
        confidence_score DOUBLE,
        classification_method STRING,
        classification_timestamp TIMESTAMP,
        file_source_path STRING,
        ai_reasoning STRING,
        processing_time_ms INT
    ) USING DELTA
    """)

print("âœ… Tables ready")

# Sample logs
sample_logs = [
    "CRITICAL Database connection lost...",
    "ERROR Disk space running low...",
    "WARN High memory usage detected...",
    "INFO System reboot scheduled..."
]

for i, log in enumerate(sample_logs):
    dbutils.fs.put(f"{LOG_SOURCE_PATH}/driver_test_{i}.log", log, True)

print("âœ… Sample logs written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ForeachBatch Processor (Driver FM Calls)

# COMMAND ----------

def process_batch_with_true_ai_hybrid(batch_df, batch_id):
    rows = batch_df.collect()  # âœ… Force to driver
    results = []
    for idx, row in enumerate(rows):
        start = time.time()
        row_dict = row.asDict()
        log_content = row_dict["log_content"]
        file_path = row_dict["file_path"]
        result = hybrid_classify(log_content)
        proc_time = int((time.time() - start) * 1000)
        results.append({
            "severity_id": f"sev_{int(time.time()*1000000)}_{idx}",
            "raw_log_content": log_content,
            "predicted_severity": result["severity"],
            "confidence_score": result["confidence"],
            "classification_method": result["method"],
            "classification_timestamp": datetime.now(),
            "file_source_path": file_path,
            "ai_reasoning": result.get("reasoning", ""),
            "processing_time_ms": proc_time
        })
    if results:
        spark.createDataFrame(results).write.format("delta").mode("append").saveAsTable(SEVERITY_TABLE)
        print(f"âœ… Wrote {len(results)} rows to {SEVERITY_TABLE}")

print("âœ… Driver-side foreachBatch ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Start Streaming

# COMMAND ----------

# MAGIC ## 5. Start Raw Ingestion Stream (Line-by-Line Ingestion)
print("ðŸŒŠ Starting raw log ingestion with proper line splitting...")

raw_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "text")
    .load(LOG_SOURCE_PATH)
    # ðŸ”‘ Split into individual log lines
    .selectExpr("explode(split(value, '\\n')) as raw_log_content", "_metadata.file_path as file_source_path")
    .filter("trim(raw_log_content) != ''")  # remove empty lines
    .withColumn("ingestion_timestamp", current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", RAW_CHECKPOINT)
    .outputMode("append")
    .toTable(RAW_LOGS_TABLE))

print("âœ… Raw stream started (line-level ingestion)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Monitor Streaming

# COMMAND ----------

import time
start_time = time.time()
while time.time() - start_time < 60:  # 1 min monitor
    raw_count = spark.table(RAW_LOGS_TABLE).count()
    sev_count = spark.table(SEVERITY_TABLE).count()
    print(f"â° Raw={raw_count}, Classified={sev_count}")
    time.sleep(15)

raw_stream.stop()
classification_stream.stop()
print("âœ… Streams stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. TRUE AI vs Rules Performance Analysis

# COMMAND ----------

print("ðŸ“Š TRUE AI vs RULES PERFORMANCE ANALYSIS")
df = spark.table(SEVERITY_TABLE)
df.groupBy("classification_method").count().show()
df.groupBy("predicted_severity").count().show()
df.select("raw_log_content","predicted_severity","classification_method","ai_reasoning").show(truncate=False)
