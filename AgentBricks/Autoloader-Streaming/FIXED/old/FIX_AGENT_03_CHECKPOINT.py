# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ”§ Fix Agent 03 Checkpoint Issue

# COMMAND ----------

print("ðŸ”§ FIXING AGENT 03 CHECKPOINT AFTER TABLE RECREATION")
print("=" * 60)

# The issue: After dropping and recreating the network_operations table,
# Agent 03's checkpoint still references the old table ID
# Solution: Clear the checkpoint location

# COMMAND ----------

# Configuration
INCIDENTS_CHECKPOINT = "/FileStore/checkpoints/incident_manager_ai_hybrid_fixed"
OPS_CHECKPOINT = "/FileStore/checkpoints/network_ops_ai_hybrid_fixed"

print("ðŸ—‘ï¸ Clearing checkpoint locations to fix streaming table ID mismatch...")

# COMMAND ----------

# Remove Agent 02 checkpoint (precautionary)
try:
    dbutils.fs.rm(INCIDENTS_CHECKPOINT, True)
    print(f"âœ… Cleared Agent 02 checkpoint: {INCIDENTS_CHECKPOINT}")
except Exception as e:
    print(f"âš ï¸ Agent 02 checkpoint already clean or error: {e}")

# COMMAND ----------

# Remove Agent 03 checkpoint (main fix)
try:
    dbutils.fs.rm(OPS_CHECKPOINT, True)
    print(f"âœ… Cleared Agent 03 checkpoint: {OPS_CHECKPOINT}")
except Exception as e:
    print(f"âš ï¸ Agent 03 checkpoint already clean or error: {e}")

# COMMAND ----------

# Also clear downstream agent checkpoints to be safe
RCA_CHECKPOINT = "/FileStore/checkpoints/rca_ai_hybrid_fixed"
ORCHESTRATOR_CHECKPOINT = "/FileStore/checkpoints/orchestrator_ai_hybrid_fixed"

try:
    dbutils.fs.rm(RCA_CHECKPOINT, True)
    print(f"âœ… Cleared Agent 04 checkpoint: {RCA_CHECKPOINT}")
except Exception as e:
    print(f"âš ï¸ Agent 04 checkpoint already clean or error: {e}")

try:
    dbutils.fs.rm(ORCHESTRATOR_CHECKPOINT, True)
    print(f"âœ… Cleared Agent 05 checkpoint: {ORCHESTRATOR_CHECKPOINT}")
except Exception as e:
    print(f"âš ï¸ Agent 05 checkpoint already clean or error: {e}")

# COMMAND ----------

print("=" * 60)
print("âœ… CHECKPOINT CLEANUP COMPLETE")
print("ðŸŽ¯ All agents will start with fresh checkpoints")
print("ðŸ“‹ Ready to re-run agents 02-05 in sequence")
print("=" * 60)

# COMMAND ----------

print("ðŸ“‹ NEXT STEPS:")
print("1. Run Agent 02: 02_Incident_Manager_AgentBricks_TRUE_AI_HYBRID_FIXED.py")
print("2. Run Agent 03: 03_Network_Ops_Agent_AgentBricks_TRUE_AI_HYBRID_FIXED.py")
print("3. Run Agent 04: 04_RCA_Agent_AgentBricks_TRUE_AI_HYBRID_FIXED.py") 
print("4. Run Agent 05: 05_Multi_Agent_Orchestrator_AgentBricks_TRUE_AI_HYBRID_FIXED.py")
print("\nðŸ”„ All checkpoints are now clean for fresh streaming execution")
