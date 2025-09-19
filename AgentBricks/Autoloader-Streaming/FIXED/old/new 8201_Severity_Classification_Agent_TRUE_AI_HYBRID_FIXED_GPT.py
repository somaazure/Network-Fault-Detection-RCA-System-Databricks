# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸš€ Severity Classification - AI + Rules Hybrid (Streaming + Performance Analysis) - FIXED (LongType)

# COMMAND ----------

import time
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import LongType
import mlflow.deployments

spark = SparkSession.builder.getOrCreate()

print("=" * 80)
print("ðŸ”§ RUNNING: 01_Severity_Classification_Agent_TRUE_AI_HYBRID_LONGTYPE.py")
print("ðŸ¤– THIS VERSION HAS FOUNDATION MODEL + RULES HYBRID LOGIC")
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
RAW_CHECKPOINT = "/FileStore/checkpoints/raw_ai_rules_raw_long"
SEV_CHECKPOINT = "/FileStore/checkpoints/raw_ai_rules_sev_long"

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
    try:
        # âœ… Recreate client per call (safe in streaming workers)
        client = mlflow.deployments.get_deploy_client("databricks")

        response = client.predict(
            endpoint=FOUNDATION_MODEL_NAME,
            inputs={
                "messages": [
                    {"role": "system", "content": "You are an expert incident classifier."},
                    {"role": "user", "content": f"Classify this log into exactly one label: P1, P2, P3, or INFO.\nLog: {log_content}"}
                ],
                "temperature": 0.1,
                "max_tokens": 20
            }
        )

        # Handle both formats
        prediction = None
        if "choices" in response:
            prediction = response["choices"][0]["message"]["content"].strip()
        elif "predictions" in response:
            candidates = response["predictions"][0].get("candidates", [])
            if candidates:
                prediction = candidates[0]["text"].strip()
            elif "generated_text" in response["predictions"][0]:
                prediction = response["predictions"][0]["generated_text"].strip()

        if not prediction:
            return {"success": False}

        prediction_upper = prediction.upper()
        for sev in ["P1", "P2", "P3", "INFO"]:
            if sev in prediction_upper:
                return {
                    "success": True,
                    "severity": sev,
                    "confidence": 0.85,
                    "method": "ai_foundation_model",
                    "reasoning": f"FM classified as {sev}"
                }

        return {"success": False}

    except Exception as e:
        print(f"âš ï¸ FM call failed inside streaming: {e}")
        return {"success": False}




def hybrid_classify(log_content: str) -> dict:
    fm_res = classify_with_fm(log_content)

    if fm_res.get("success"):
        print(f"âœ… Using FM result: {fm_res['severity']}")
        return fm_res
    else:
        print("âš ï¸ Falling back to rules")
        return classify_with_rules(log_content)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Setup Tables + Sample Logs

# COMMAND ----------

# Drop and recreate severity table with BIGINT for processing_time_ms
spark.sql(f"DROP TABLE IF EXISTS {SEVERITY_TABLE}")

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
    processing_time_ms BIGINT
) USING DELTA
""")

# Ensure raw logs table exists
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

# Clean checkpoints and logs
try:
    dbutils.fs.rm(LOG_SOURCE_PATH, recurse=True)
    dbutils.fs.rm(RAW_CHECKPOINT, recurse=True)
    dbutils.fs.rm(SEV_CHECKPOINT, recurse=True)
except:
    pass

print("âœ… Tables recreated with BIGINT for processing_time_ms")

# Sample logs
sample_logs = [
    "CRITICAL Database connection lost",
    "ERROR Disk space running low",
    "WARN High memory usage detected",
    "INFO System reboot scheduled"
]
dbutils.fs.put(f"{LOG_SOURCE_PATH}/ai_test_logs.log", "\n".join(sample_logs), True)
print(f"âœ… Sample logs created: {len(sample_logs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ForeachBatch Processor

# COMMAND ----------

def process_batch_with_true_ai_hybrid(batch_df, batch_id):
    rows = batch_df.collect()
    results = []
    for idx, row in enumerate(rows):
        start_time = time.time()
        row_dict = row.asDict()
        result = hybrid_classify(row_dict["log_content"])
        processing_time = int((time.time() - start_time) * 1000)
        results.append({
            "severity_id": f"sev_{int(time.time()*1000000)}_{idx}",
            "raw_log_content": row_dict["log_content"],
            "predicted_severity": result["severity"],
            "confidence_score": result["confidence"],
            "classification_method": result["method"],
            "classification_timestamp": datetime.now(),
            "file_source_path": row_dict["file_path"],
            "ai_reasoning": result.get("reasoning", ""),
            "processing_time_ms": processing_time
        })
    if results:
        results_df = spark.createDataFrame(results)
        aligned_df = results_df.select(
            "severity_id","raw_log_content","predicted_severity","confidence_score",
            "classification_method","classification_timestamp","file_source_path",
            "ai_reasoning", col("processing_time_ms").cast(LongType()).alias("processing_time_ms")
        )
        aligned_df.write.format("delta").mode("append").saveAsTable(SEVERITY_TABLE)
        print(f"âœ… Wrote {len(results)} results")

print("âœ… Processor ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Start Streaming

# COMMAND ----------

raw_stream = (spark.readStream
    .format("cloudFiles").option("cloudFiles.format", "text")
    .load(LOG_SOURCE_PATH)
    .withColumn("log_content", col("value"))
    .withColumn("file_path", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .drop("value")
    .writeStream
    .format("delta")
    .option("checkpointLocation", RAW_CHECKPOINT)
    .toTable(RAW_LOGS_TABLE))

classification_stream = (spark.readStream
    .format("delta").table(RAW_LOGS_TABLE)
    .writeStream
    .foreachBatch(process_batch_with_true_ai_hybrid)
    .option("checkpointLocation", SEV_CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start())

print("âœ… Streaming started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Monitor Streaming

# COMMAND ----------

import time
start = time.time()
while time.time() - start < 60:
    raw_count = spark.table(RAW_LOGS_TABLE).count()
    sev_count = spark.table(SEVERITY_TABLE).count()
    print(f"Raw={raw_count}, Classified={sev_count}")
    time.sleep(10)

classification_stream.stop()
raw_stream.stop()
print("âœ… Streams stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Performance Analysis

# COMMAND ----------

display(spark.table(SEVERITY_TABLE))
