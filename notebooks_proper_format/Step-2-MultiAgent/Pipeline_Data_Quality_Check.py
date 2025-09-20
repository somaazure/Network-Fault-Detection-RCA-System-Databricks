# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Data Quality Check
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

import os
# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Pipeline Data Quality Check & Monitoring
# MAGIC
# MAGIC **Purpose**: Validate data quality and pipeline health after each execution
# MAGIC **Scope**: All 5 agent tables + summary statistics + data lineage validation

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import json

spark = SparkSession.builder.getOrCreate()

print("📊 Pipeline Data Quality Check & Monitoring")
print("=" * 70)

# COMMAND ----------

# Configuration
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"

TABLES = {
    "severity_classifications_streaming": "Agent 01: Severity Classification",
    "incident_decisions_streaming": "Agent 02: Incident Manager",
    "network_operations_streaming": "Agent 03: Network Operations",
    "rca_reports_streaming": "Agent 04: RCA Analysis",
    "multi_agent_workflows_streaming": "Agent 05: Multi-Agent Orchestrator"
}

# COMMAND ----------

def get_table_stats(table_name):
    """Get comprehensive statistics for a table"""
    full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"

    try:
        df = spark.table(full_table_name)

        # Basic statistics
        total_count = df.count()

        # Recent data (last hour)
        recent_df = df.filter(col("created_timestamp") > (current_timestamp() - expr("INTERVAL 1 HOURS")))
        recent_count = recent_df.count()

        # Get latest timestamp
        latest_ts = df.agg(max("created_timestamp").alias("latest")).collect()[0]["latest"]

        # Get data freshness (minutes since latest record)
        if latest_ts:
            freshness_minutes = (datetime.now() - latest_ts).total_seconds() / 60
        else:
            freshness_minutes = None

        return {
            "table": table_name,
            "total_records": total_count,
            "recent_records": recent_count,
            "latest_timestamp": latest_ts,
            "freshness_minutes": freshness_minutes,
            "status": "✅" if total_count > 0 else "❌"
        }

    except Exception as e:
        return {
            "table": table_name,
            "error": str(e),
            "status": "❌"
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Assessment

# COMMAND ----------

print("🔍 PIPELINE DATA QUALITY ASSESSMENT")
print("=" * 70)

# Collect statistics for all tables
table_stats = []
total_pipeline_records = 0
healthy_tables = 0

for table_name, description in TABLES.items():
    print(f"\n📋 {description}")
    print("-" * 50)

    stats = get_table_stats(table_name)
    table_stats.append(stats)

    if "error" in stats:
        print(f"❌ Error accessing table: {stats['error']}")
    else:
        print(f"📊 Total Records: {stats['total_records']:,}")
        print(f"🕐 Recent Records (last hour): {stats['recent_records']:,}")
        print(f"⏰ Latest Timestamp: {stats['latest_timestamp']}")

        if stats['freshness_minutes'] is not None:
            print(f"🔄 Data Freshness: {stats['freshness_minutes']:.1f} minutes ago")

        print(f"✅ Status: {stats['status']}")

        if stats['total_records'] > 0:
            total_pipeline_records += stats['total_records']
            healthy_tables += 1

print("\n" + "=" * 70)
print("📈 PIPELINE SUMMARY")
print("=" * 70)
print(f"🗃️  Healthy Tables: {healthy_tables}/{len(TABLES)}")
print(f"📊 Total Pipeline Records: {total_pipeline_records:,}")
print(f"📋 Average Records per Table: {total_pipeline_records/len(TABLES):.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage Validation

# COMMAND ----------

print("🔗 DATA LINEAGE VALIDATION")
print("=" * 70)

# Check data flow consistency
def validate_data_lineage():
    """Validate that data flows correctly between agents"""
    lineage_results = {}

    try:
        # Get record counts for each table
        severity_count = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.severity_classifications_streaming").count()
        incident_count = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.incident_decisions_streaming").count()
        ops_count = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.network_operations_streaming").count()
        rca_count = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_reports_streaming").count()
        workflow_count = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.multi_agent_workflows_streaming").count()

        # Validate 1:1 relationships (all should have same count for healthy pipeline)
        expected_count = severity_count  # Base count from first agent

        lineage_results = {
            "severity_classifications": {"count": severity_count, "status": "✅"},
            "incident_decisions": {
                "count": incident_count,
                "status": "✅" if incident_count == expected_count else "⚠️"
            },
            "network_operations": {
                "count": ops_count,
                "status": "✅" if ops_count == expected_count else "⚠️"
            },
            "rca_reports": {
                "count": rca_count,
                "status": "✅" if rca_count == expected_count else "⚠️"
            },
            "multi_agent_workflows": {
                "count": workflow_count,
                "status": "✅" if workflow_count == expected_count else "⚠️"
            }
        }

        # Display results
        for table, results in lineage_results.items():
            status_icon = results["status"]
            count = results["count"]
            print(f"{status_icon} {table}: {count:,} records")

        # Overall lineage health
        all_healthy = all(r["status"] == "✅" for r in lineage_results.values())
        print(f"\n🔗 Data Lineage Health: {'✅ HEALTHY' if all_healthy else '⚠️ ISSUES DETECTED'}")

        return lineage_results

    except Exception as e:
        print(f"❌ Error validating data lineage: {str(e)}")
        return {}

lineage_results = validate_data_lineage()

# COMMAND ----------

# MAGIC %md
# MAGIC ## RCA Content Quality Check (RAG Preparation)

# COMMAND ----------

print("🎯 RCA CONTENT QUALITY CHECK (RAG PREPARATION)")
print("=" * 70)

try:
    rca_df = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_reports_streaming")
    rca_count = rca_df.count()

    if rca_count > 0:
        print(f"📊 Total RCA Reports: {rca_count:,}")

        # Analyze RCA content quality
        content_stats = rca_df.select(
            avg(length(col("root_cause_analysis"))).alias("avg_content_length"),
            min(length(col("root_cause_analysis"))).alias("min_content_length"),
            max(length(col("root_cause_analysis"))).alias("max_content_length"),
            count(when(col("root_cause_analysis").isNotNull() & (length(col("root_cause_analysis")) > 100), True)).alias("quality_content_count")
        ).collect()[0]

        avg_length = content_stats["avg_content_length"]
        min_length = content_stats["min_content_length"]
        max_length = content_stats["max_content_length"]
        quality_count = content_stats["quality_content_count"]

        print(f"📝 Average Content Length: {avg_length:.0f} characters")
        print(f"📏 Content Length Range: {min_length} - {max_length} characters")
        print(f"✅ Quality Content (>100 chars): {quality_count:,} reports ({(quality_count/rca_count)*100:.1f}%)")

        # Sample RCA content for manual review
        print("\n🔍 Sample RCA Content Preview:")
        sample_rca = rca_df.select("root_cause_analysis").limit(2).collect()
        for i, row in enumerate(sample_rca, 1):
            content = row["root_cause_analysis"][:200] + "..." if len(row["root_cause_analysis"]) > 200 else row["root_cause_analysis"]
            print(f"   Sample {i}: {content}")

        # RAG readiness assessment
        rag_readiness = (quality_count / rca_count) >= 0.8  # 80% threshold
        print(f"\n🎯 RAG Readiness: {'✅ READY' if rag_readiness else '⚠️ NEEDS IMPROVEMENT'}")

    else:
        print("❌ No RCA reports found")

except Exception as e:
    print(f"❌ Error checking RCA content: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Metrics

# COMMAND ----------

print("⚡ PIPELINE PERFORMANCE METRICS")
print("=" * 70)

try:
    # Calculate processing timestamps across the pipeline
    severity_df = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.severity_classifications_streaming")
    workflow_df = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.multi_agent_workflows_streaming")

    # Get timestamp ranges
    if severity_df.count() > 0 and workflow_df.count() > 0:
        severity_times = severity_df.agg(
            min("created_timestamp").alias("first_severity"),
            max("created_timestamp").alias("last_severity")
        ).collect()[0]

        workflow_times = workflow_df.agg(
            min("created_timestamp").alias("first_workflow"),
            max("created_timestamp").alias("last_workflow")
        ).collect()[0]

        # Calculate end-to-end processing time
        if severity_times["first_severity"] and workflow_times["last_workflow"]:
            processing_duration = (workflow_times["last_workflow"] - severity_times["first_severity"]).total_seconds()
            print(f"⏱️ End-to-End Processing Time: {processing_duration:.1f} seconds ({processing_duration/60:.1f} minutes)")

        # Records per second
        if processing_duration > 0:
            records_per_second = total_pipeline_records / processing_duration
            print(f"📈 Pipeline Throughput: {records_per_second:.2f} records/second")

        print(f"🕐 First Severity Classification: {severity_times['first_severity']}")
        print(f"🕐 Last Workflow Completion: {workflow_times['last_workflow']}")

except Exception as e:
    print(f"❌ Error calculating performance metrics: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

# Generate final pipeline health report
print("📋 FINAL PIPELINE HEALTH REPORT")
print("=" * 70)

pipeline_health_score = (healthy_tables / len(TABLES)) * 100
print(f"🎯 Pipeline Health Score: {pipeline_health_score:.1f}%")

if pipeline_health_score == 100:
    print("✅ EXCELLENT: All agents processing successfully")
    recommendation = "Pipeline is production-ready for RAG implementation"
elif pipeline_health_score >= 80:
    print("⚠️ GOOD: Most agents working, minor issues detected")
    recommendation = "Address issues before RAG implementation"
else:
    print("❌ CRITICAL: Multiple agents failing")
    recommendation = "Fix pipeline issues before proceeding"

print(f"💡 Recommendation: {recommendation}")

# Export statistics for monitoring dashboard
pipeline_report = {
    "timestamp": datetime.now().isoformat(),
    "health_score": pipeline_health_score,
    "total_records": total_pipeline_records,
    "healthy_tables": healthy_tables,
    "table_stats": table_stats,
    "recommendation": recommendation
}

# Save report as JSON (optional for external monitoring)
print(f"\n📄 Pipeline report generated at: {datetime.now()}")

# COMMAND ----------

# Optional: Display tables for manual inspection
print("🔍 TABLE INSPECTION (First 5 rows of each table)")
print("=" * 70)

for table_name, description in TABLES.items():
    try:
        print(f"\n📋 {description} ({table_name})")
        df = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}")
        df.limit(5).display()
    except Exception as e:
        print(f"❌ Cannot display {table_name}: {str(e)}")