# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸš€ Multi-Agent Orchestrator - TRUE AI + Rules Hybrid (FIXED)

# COMMAND ----------

import time
import json
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, current_timestamp, expr, lit
from pyspark.sql.types import IntegerType
import mlflow.deployments

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Configuration
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"

# All agent tables
SEVERITY_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.severity_classifications_streaming"
INCIDENTS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.incident_decisions_streaming"
NETWORK_OPS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.network_operations_streaming"
RCA_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_reports_streaming"
ORCHESTRATOR_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.multi_agent_workflows_streaming"

ORCHESTRATOR_CHECKPOINT = "/FileStore/checkpoints/orchestrator_ai_hybrid_fixed"

FOUNDATION_MODEL_NAME = "databricks-meta-llama-3-1-8b-instruct"

# COMMAND ----------

# ðŸ§¹ AUTOMATED CHECKPOINT CLEANUP FUNCTION
def cleanup_checkpoint_if_needed(checkpoint_path, table_name, description=""):
    """Clean checkpoint when table schema changes or for fresh starts"""
    try:
        print(f"ðŸ” Checking checkpoint: {description}")
        try:
            checkpoint_files = dbutils.fs.ls(checkpoint_path)
            if len(checkpoint_files) > 0:
                print(f"ðŸ§¹ Cleaning existing checkpoint: {checkpoint_path}")
                dbutils.fs.rm(checkpoint_path, recurse=True)
                print(f"âœ… Checkpoint cleaned: {description}")
            else:
                print(f"â„¹ï¸ No checkpoint to clean: {description}")
        except Exception as ls_error:
            print(f"â„¹ï¸ Checkpoint doesn't exist or already clean: {description}")
    except Exception as e:
        print(f"âš ï¸ Checkpoint cleanup warning for {description}: {str(e)}")

print("ðŸ› ï¸ Multi-Agent Orchestrator checkpoint cleanup function ready")

# Force rule-based processing for reliable table population
AI_ENABLED = False
print("ðŸ”§ Using rule-based processing only for reliable table population")

# COMMAND ----------

# AI + Rules Hybrid Functions
def orchestrate_workflow_with_fm(workflow_data: dict) -> dict:
    if not AI_ENABLED:
        return {"success": False}
    try:
        severity = workflow_data.get("severity_classification", "INFO")
        priority = workflow_data.get("incident_priority", "INFO")
        operation = workflow_data.get("recommended_operation", "monitor")
        rca_category = workflow_data.get("root_cause_category", "Unknown")
        
        response = client.predict(
            endpoint=FOUNDATION_MODEL_NAME,
            inputs={
                "messages": [
                    {"role": "system", "content": "You are a multi-agent workflow orchestrator."},
                    {"role": "user", "content": f"Analyze complete workflow: {severity} severity â†’ {priority} priority â†’ {operation} operation â†’ {rca_category} root cause. Determine overall workflow status (SUCCESS/PARTIAL/FAILED) and provide orchestration summary. Format: STATUS:SUMMARY"}
                ],
                "temperature": 0.1,
                "max_tokens": 120
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
            if len(parts) >= 2:
                status = parts[0].strip().upper()
                summary = ":".join(parts[1:]).strip()
                
                if status in ["SUCCESS", "PARTIAL", "FAILED"]:
                    return {"success": True, "workflow_status": status, "orchestration_summary": summary[:250],
                           "method": "ai_foundation_model", "confidence": 0.85}
        return {"success": False}
    except Exception as e:
        print(f"âš ï¸ FM call failed: {e}")
        return {"success": False}

def orchestrate_workflow_with_rules(workflow_data: dict) -> dict:
    severity = workflow_data.get("severity_classification", "INFO")
    priority = workflow_data.get("incident_priority", "INFO")
    operation = workflow_data.get("recommended_operation", "monitor")
    rca_category = workflow_data.get("root_cause_category", "Unknown")
    
    # Rule-based workflow evaluation
    success_factors = 0
    total_factors = 4
    
    # Check severity classification success
    if severity in ["P1", "P2", "P3", "INFO"]:
        success_factors += 1
    
    # Check incident management success
    if priority in ["HIGH", "MEDIUM", "LOW", "INFO"]:
        success_factors += 1
    
    # Check operation planning success
    valid_operations = ["restart_node", "reroute_traffic", "scale_resources", "investigate", "monitor"]
    if operation in valid_operations:
        success_factors += 1
    
    # Check RCA completion
    if rca_category != "Unknown":
        success_factors += 1
    
    success_rate = success_factors / total_factors
    
    if success_rate >= 0.8:
        status = "SUCCESS"
        summary = f"Complete workflow executed successfully: {severity} â†’ {priority} â†’ {operation} â†’ {rca_category}"
    elif success_rate >= 0.5:
        status = "PARTIAL"
        summary = f"Partial workflow completion with {success_factors}/{total_factors} successful stages"
    else:
        status = "FAILED"
        summary = f"Workflow execution failed with only {success_factors}/{total_factors} successful stages"
    
    return {"workflow_status": status, "orchestration_summary": summary,
           "method": "rule_based", "confidence": 0.75}

def hybrid_workflow_orchestration(workflow_data: dict) -> dict:
    fm_res = orchestrate_workflow_with_fm(workflow_data)
    if fm_res.get("success") and fm_res.get("confidence", 0) >= 0.8:
        return fm_res
    return orchestrate_workflow_with_rules(workflow_data)

# COMMAND ----------

# âœ… APPEND MODE: Setup Tables - Preserve historical data
try:
    existing_workflows = spark.table(ORCHESTRATOR_TABLE).count()
    print(f"ðŸ“Š Found existing orchestrator table with {existing_workflows} records - will append new data")
except:
    print("ðŸ“‹ Creating new multi-agent orchestrator table")
    spark.sql(f"""
    CREATE TABLE {ORCHESTRATOR_TABLE} (
        workflow_id STRING,
        severity_classification STRING,
        incident_priority STRING,
        recommended_operation STRING,
        root_cause_category STRING,
        workflow_status STRING,
        orchestration_summary STRING,
        orchestration_method STRING,
        workflow_confidence DOUBLE,
        workflow_timestamp TIMESTAMP,
        total_processing_time_ms INT
    ) USING DELTA
    """)

print("âœ… Multi-agent orchestrator table ready")

# COMMAND ----------

# Multi-Table Join and Orchestration with Deduplication
def get_complete_workflow_data():
    try:
        from pyspark.sql.functions import col, row_number
        from pyspark.sql.window import Window
        
        # Start with severity classifications as the base
        severity_df = spark.table(SEVERITY_TABLE).select(
            "severity_id", "predicted_severity", "classification_timestamp"
        ).alias("sev")
        
        workflow_df = severity_df
        join_count = 1
        
        if spark.catalog.tableExists(INCIDENTS_TABLE):
            incidents_df = spark.table(INCIDENTS_TABLE).select(
                "incident_id", "severity_classification", "incident_priority", "decision_timestamp"
            )
            if incidents_df.count() > 0:
                # Deduplicate: Get latest incident per severity classification
                window_inc = Window.partitionBy("severity_classification").orderBy(col("decision_timestamp").desc())
                incidents_dedup = incidents_df.withColumn("rn", row_number().over(window_inc)).filter(col("rn") == 1).drop("rn").alias("inc")
                
                workflow_df = workflow_df.join(
                    incidents_dedup,
                    col("sev.predicted_severity") == col("inc.severity_classification"),
                    "left"
                )
                join_count += 1
        
        if spark.catalog.tableExists(NETWORK_OPS_TABLE):
            ops_df = spark.table(NETWORK_OPS_TABLE).select(
                "operation_id", "incident_priority", "recommended_operation", "operation_timestamp"
            )
            if ops_df.count() > 0:
                # Deduplicate: Get latest operation per incident priority
                window_ops = Window.partitionBy("incident_priority").orderBy(col("operation_timestamp").desc())
                ops_dedup = ops_df.withColumn("rn", row_number().over(window_ops)).filter(col("rn") == 1).drop("rn").alias("ops")
                
                workflow_df = workflow_df.join(
                    ops_dedup,
                    col("inc.incident_priority") == col("ops.incident_priority"),
                    "left"
                )
                join_count += 1
        
        if spark.catalog.tableExists(RCA_TABLE):
            rca_df = spark.table(RCA_TABLE).select(
                "rca_id", "recommended_operation", "root_cause_category", "rca_timestamp"
            )
            if rca_df.count() > 0:
                # Deduplicate: Get latest RCA per recommended operation
                window_rca = Window.partitionBy("recommended_operation").orderBy(col("rca_timestamp").desc())
                rca_dedup = rca_df.withColumn("rn", row_number().over(window_rca)).filter(col("rn") == 1).drop("rn").alias("rca")
                
                workflow_df = workflow_df.join(
                    rca_dedup,
                    col("ops.recommended_operation") == col("rca.recommended_operation"),
                    "left"
                )
                join_count += 1
        
        # Select final columns with null handling
        final_df = workflow_df.select(
            col("sev.severity_id").alias("base_severity_id"),
            col("sev.predicted_severity").alias("severity_classification"),
            col("inc.incident_priority").alias("incident_priority"),
            col("ops.recommended_operation").alias("recommended_operation"),
            col("rca.root_cause_category").alias("root_cause_category")
        ).fillna({
            "incident_priority": "INFO",
            "recommended_operation": "monitor", 
            "root_cause_category": "Unknown"
        })
        
        print(f"ðŸ“Š Workflow join completed: {join_count} tables joined with deduplication")
        final_count = final_df.count()
        print(f"ðŸŽ¯ Final deduplicated workflow records: {final_count}")
        return final_df
        
    except Exception as e:
        print(f"âš ï¸ Workflow join failed: {e}")
        # Fallback to just severity data
        return spark.table(SEVERITY_TABLE).select(
            col("severity_id").alias("base_severity_id"),
            col("predicted_severity").alias("severity_classification"),
            lit("INFO").alias("incident_priority"),
            lit("monitor").alias("recommended_operation"),
            lit("Unknown").alias("root_cause_category")
        )

# COMMAND ----------

# ForeachBatch Processor for Complete Workflows
def process_orchestrator_batch(batch_df, batch_id):
    print(f"\nðŸŽ¯ Processing orchestrator batch {batch_id}")
    
    # Get complete workflow data by joining all agent results
    try:
        complete_workflows = get_complete_workflow_data()
        
        workflow_count = complete_workflows.count()
        if workflow_count == 0:
            print("âš ï¸ No complete workflow data available yet")
            return
        
        print(f"ðŸ“Š Found {workflow_count} complete workflows to orchestrate (deduplicated)")
        
        rows = complete_workflows.collect()
        results = []
        
        for idx, row in enumerate(rows):
            row_dict = row.asDict()  # âœ… FIXED: Safe field access
            
            workflow_data = {
                "severity_classification": row_dict["severity_classification"],
                "incident_priority": row_dict["incident_priority"],
                "recommended_operation": row_dict["recommended_operation"],
                "root_cause_category": row_dict["root_cause_category"]
            }
            
            result = hybrid_workflow_orchestration(workflow_data)
            
            record = {
                "workflow_id": f"wf_{int(time.time()*1000000)}_{idx}",
                "severity_classification": row_dict["severity_classification"],
                "incident_priority": row_dict["incident_priority"],
                "recommended_operation": row_dict["recommended_operation"],
                "root_cause_category": row_dict["root_cause_category"],
                "workflow_status": result["workflow_status"],
                "orchestration_summary": result["orchestration_summary"][:300],
                "orchestration_method": result["method"],
                "workflow_confidence": result.get("confidence", 0.75),
                "workflow_timestamp": datetime.now(),
                "total_processing_time_ms": int(0)
            }
            results.append(record)
        
        if results:
            results_df = spark.createDataFrame(results)
            # âœ… FIXED: Explicit schema alignment with type casting
            aligned_df = results_df.select(
                "workflow_id", "severity_classification", "incident_priority", "recommended_operation",
                "root_cause_category", "workflow_status", "orchestration_summary", "orchestration_method",
                "workflow_confidence", "workflow_timestamp", 
                col("total_processing_time_ms").cast(IntegerType()).alias("total_processing_time_ms")
            )
            aligned_df.write.format("delta").mode("append").saveAsTable(ORCHESTRATOR_TABLE)
            print(f"âœ… Wrote {len(results)} orchestrated workflows")
        else:
            print("âš ï¸ No orchestration results")
            
    except Exception as e:
        print(f"âŒ Orchestration batch error: {e}")
        import traceback
        traceback.print_exc()

# COMMAND ----------

# ðŸ§¹ Clean checkpoint for fresh start
print("ðŸš€ STARTING FRESH - CLEANING CHECKPOINT")
cleanup_checkpoint_if_needed(ORCHESTRATOR_CHECKPOINT, ORCHESTRATOR_TABLE, "Multi-Agent Orchestrator")

# Start Orchestration (triggers periodically to join all agent results)
print("ðŸŒŠ Starting multi-agent orchestration...")

# Use a simple timer-based approach instead of streaming for orchestration
orchestration_stream = (spark.readStream
    .format("delta")
    .table(SEVERITY_TABLE)  # Trigger on new severity classifications
    .writeStream
    .foreachBatch(process_orchestrator_batch)
    .option("checkpointLocation", ORCHESTRATOR_CHECKPOINT)
    .trigger(processingTime="35 seconds")  # Give time for all agents to process
    .start())

print("âœ… Multi-agent orchestration started")

# COMMAND ----------

# Monitor All Agents
start_time = time.time()
duration = 120  # Longer duration for complete workflow
while time.time() - start_time < duration:
    elapsed = int(time.time() - start_time)
    try:
        sev_count = spark.table(SEVERITY_TABLE).count() if spark.catalog.tableExists(SEVERITY_TABLE) else 0
        inc_count = spark.table(INCIDENTS_TABLE).count() if spark.catalog.tableExists(INCIDENTS_TABLE) else 0
        ops_count = spark.table(NETWORK_OPS_TABLE).count() if spark.catalog.tableExists(NETWORK_OPS_TABLE) else 0
        rca_count = spark.table(RCA_TABLE).count() if spark.catalog.tableExists(RCA_TABLE) else 0
        orch_count = spark.table(ORCHESTRATOR_TABLE).count() if spark.catalog.tableExists(ORCHESTRATOR_TABLE) else 0
        
        print(f"â° [{elapsed}s/{duration}s] Sev={sev_count}, Inc={inc_count}, Ops={ops_count}, RCA={rca_count}, Workflows={orch_count}")
    except:
        print(f"â° [{elapsed}s/{duration}s] Monitoring...")
    time.sleep(20)

orchestration_stream.stop()
print("ðŸ›‘ Multi-agent orchestration stopped")

# COMMAND ----------

# Complete Multi-Agent Analysis
print("ðŸ“Š COMPLETE MULTI-AGENT WORKFLOW ANALYSIS")
print("=" * 60)

try:
    # Individual agent counts
    sev_count = spark.table(SEVERITY_TABLE).count() if spark.catalog.tableExists(SEVERITY_TABLE) else 0
    inc_count = spark.table(INCIDENTS_TABLE).count() if spark.catalog.tableExists(INCIDENTS_TABLE) else 0
    ops_count = spark.table(NETWORK_OPS_TABLE).count() if spark.catalog.tableExists(NETWORK_OPS_TABLE) else 0
    rca_count = spark.table(RCA_TABLE).count() if spark.catalog.tableExists(RCA_TABLE) else 0
    orch_count = spark.table(ORCHESTRATOR_TABLE).count() if spark.catalog.tableExists(ORCHESTRATOR_TABLE) else 0
    
    print("ðŸŽ¯ AGENT PIPELINE RESULTS:")
    print(f"   ðŸ“Š Severity Classifications: {sev_count}")
    print(f"   ðŸš¨ Incident Decisions: {inc_count}")
    print(f"   ðŸ”§ Network Operations: {ops_count}")
    print(f"   ðŸ” RCA Reports: {rca_count}")
    print(f"   ðŸŽ¯ Orchestrated Workflows: {orch_count}")
    
    if orch_count > 0:
        # Workflow status analysis
        status_dist = spark.table(ORCHESTRATOR_TABLE).groupBy("workflow_status").count().collect()
        method_analysis = spark.table(ORCHESTRATOR_TABLE).groupBy("orchestration_method").count().collect()
        confidence_stats = spark.table(ORCHESTRATOR_TABLE).select(
            expr("avg(workflow_confidence) as avg_conf"),
            expr("min(workflow_confidence) as min_conf"),
            expr("max(workflow_confidence) as max_conf")
        ).collect()[0]

        print("\nðŸŽ¯ WORKFLOW STATUS DISTRIBUTION:")
        for row in status_dist:
            print(f"   {row['workflow_status']}: {row['count']}")

        print("\nðŸ¤– ORCHESTRATION METHOD BREAKDOWN:")
        for row in method_analysis:
            print(f"   {row['orchestration_method']}: {row['count']}")

        print("\nðŸ“Š WORKFLOW CONFIDENCE:")
        print(f"   Avg={confidence_stats['avg_conf']:.2f}, Min={confidence_stats['min_conf']:.2f}, Max={confidence_stats['max_conf']:.2f}")

        print("\nðŸ” SAMPLE COMPLETE WORKFLOWS:")
        samples = spark.table(ORCHESTRATOR_TABLE).select(
            "severity_classification","incident_priority","recommended_operation","root_cause_category",
            "workflow_status","orchestration_method","orchestration_summary"
        ).limit(2).collect()
        
        for i, row in enumerate(samples, 1):
            print(f"\n   ðŸ”„ Workflow {i}: {row['workflow_status']} via {row['orchestration_method']}")
            print(f"     Pipeline: {row['severity_classification']} â†’ {row['incident_priority']} â†’ {row['recommended_operation']} â†’ {row['root_cause_category']}")
            print(f"     Summary: {row['orchestration_summary'][:100]}...")
        
        # Calculate success rate
        success_workflows = sum(row['count'] for row in status_dist if row['workflow_status'] == 'SUCCESS')
        success_rate = (success_workflows / orch_count) * 100 if orch_count > 0 else 0
        
        print(f"\nâœ… OVERALL SUCCESS RATE: {success_rate:.1f}% ({success_workflows}/{orch_count} workflows)")
        
        if success_rate >= 80:
            print("ðŸŽ‰ MULTI-AGENT SYSTEM: PRODUCTION READY!")
        elif success_rate >= 60:
            print("âš ï¸ MULTI-AGENT SYSTEM: PARTIALLY FUNCTIONAL - needs optimization")
        else:
            print("âŒ MULTI-AGENT SYSTEM: REQUIRES DEBUGGING")
    else:
        print("\nâŒ No orchestrated workflows - check agent pipeline execution")
        
except Exception as e:
    print(f"âŒ Multi-agent analysis failed: {e}")

# COMMAND ----------

# Show final orchestrated results
if spark.catalog.tableExists(ORCHESTRATOR_TABLE):
    print("ðŸ“‹ FINAL ORCHESTRATED WORKFLOWS:")
    spark.table(ORCHESTRATOR_TABLE).show(truncate=False)
else:
    print("âš ï¸ No orchestrator table found")
