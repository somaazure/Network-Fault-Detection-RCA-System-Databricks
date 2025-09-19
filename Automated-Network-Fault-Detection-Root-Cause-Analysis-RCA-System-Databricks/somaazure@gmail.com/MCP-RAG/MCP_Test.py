# Databricks notebook source
!pip install databricks-vectorsearch>=0.22 mlflow>=2.8.0 Flask==2.3.3 pandas==1.5.3

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

export DATABRICKS_HOST="https://YOUR_WORKSPACE.cloud.databricks.com/"
export DATABRICKS_TOKEN="YOUR_DATABRICKS_TOKEN"
export VECTOR_INDEX_NAME="network_fault_detection.processed_data.rca_reports_vector_index"
export VECTOR_ENDPOINT="network_fault_detection_vs_endpoint"

# COMMAND ----------

# Run this in a Databricks notebook cell:
import os
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "your-workspace.cloud.databricks.com")

print(f"ðŸ” Cluster ID: {cluster_id}")
print(f"ðŸŒ Workspace URL: {workspace_url}")

# Your Flask API URL will be:
flask_url = f"https://{workspace_url}/driver-proxy/o/0/{cluster_id}/8080/"
print(f"ðŸ“¡ Flask API URL: {flask_url}")
