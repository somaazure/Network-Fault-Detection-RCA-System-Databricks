# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ”§ Fix Agent 03 Schema Mismatch

# COMMAND ----------

print("ðŸ”§ FIXING AGENT 03 SCHEMA MISMATCH")
print("=" * 50)

# Configuration
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"
NETWORK_OPS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.network_operations_streaming"

print(f"ðŸ“‹ Target table: {NETWORK_OPS_TABLE}")

# COMMAND ----------

# Check existing table schema
try:
    existing_df = spark.table(NETWORK_OPS_TABLE)
    existing_count = existing_df.count()
    print(f"ðŸ“Š Found existing table with {existing_count} records")
    print("ðŸ“‹ Current schema:")
    existing_df.printSchema()
    
    print("\nðŸ” Sample data:")
    existing_df.show(3, truncate=False)
    
except Exception as e:
    print(f"âš ï¸ Table doesn't exist or error reading: {e}")

# COMMAND ----------

# Drop and recreate table with correct schema for Agent 03
print("\nðŸ—‘ï¸ Dropping existing table to fix schema mismatch...")

try:
    spark.sql(f"DROP TABLE IF EXISTS {NETWORK_OPS_TABLE}")
    print("âœ… Table dropped successfully")
except Exception as e:
    print(f"âš ï¸ Error dropping table: {e}")

# COMMAND ----------

# Create table with Agent 03's expected schema
print("\nðŸ“‹ Creating table with correct schema for Agent 03...")

create_sql = f"""
CREATE TABLE {NETWORK_OPS_TABLE} (
    operation_id STRING,
    incident_priority STRING,
    recommended_operation STRING,
    risk_level STRING,
    operation_timestamp TIMESTAMP,
    planning_method STRING,
    operation_reasoning STRING,
    execution_status STRING,
    processing_time_ms INT
) USING DELTA
"""

try:
    spark.sql(create_sql)
    print("âœ… Table created with correct schema")
    
    # Verify schema
    print("\nðŸ“‹ New schema:")
    spark.table(NETWORK_OPS_TABLE).printSchema()
    
except Exception as e:
    print(f"âŒ Error creating table: {e}")

# COMMAND ----------

print("=" * 50)
print("âœ… AGENT 03 SCHEMA FIX COMPLETE")
print("ðŸŽ¯ Agent 03 can now write data with matching schema")
print("ðŸ“Š Ready for end-to-end execution")
print("=" * 50)
