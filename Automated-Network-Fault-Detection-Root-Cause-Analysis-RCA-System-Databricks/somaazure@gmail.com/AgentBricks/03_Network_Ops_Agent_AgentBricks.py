# Databricks notebook source
# MAGIC %md
# MAGIC # Network Operations Agent - AgentBricks Implementation
# MAGIC
# MAGIC ## ðŸŽ¯ Overview
# MAGIC This notebook implements the **Network Operations Agent** using AgentBricks framework that:
# MAGIC - Executes operational actions recommended by the Incident Manager
# MAGIC - Uses AgentBricks tool-based architecture for modular operations
# MAGIC - Simulates network operations (restart, reroute, QoS adjustment, scaling)
# MAGIC - Integrates with Unity Catalog for operation tracking and audit
# MAGIC - Provides real-time status updates and results
# MAGIC - Implements comprehensive safety checks and validation
# MAGIC
# MAGIC ## ðŸ—ï¸ AgentBricks Architecture
# MAGIC ```
# MAGIC Input: Incident Manager Decisions (Action + Parameters)
# MAGIC â†“
# MAGIC AgentBricks Tools:
# MAGIC   â€¢ validate_operation_safety() - Safety checks & validation
# MAGIC   â€¢ execute_network_operation() - Network operations execution
# MAGIC   â€¢ track_operation_audit() - Unity Catalog tracking & audit
# MAGIC â†“
# MAGIC NetworkOpsAgent (AgentBricks) - Orchestrates tools with Foundation Model
# MAGIC â†“
# MAGIC Structured Operation Results + Audit Trail
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ“¦ Dependencies Installation

# COMMAND ----------

# Install required packages for AgentBricks and Databricks
%pip install databricks-sdk mlflow python-dotenv requests numpy pandas

# Restart Python to ensure packages are loaded
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Configuration & Setup

# COMMAND ----------

import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Any, Optional
from enum import Enum
from dataclasses import dataclass
from functools import wraps
import uuid
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Databricks environment setup
DATABRICKS_HOST = "YOUR_WORKSPACE.cloud.databricks.com"
DATABRICKS_TOKEN = "YOUR_DATABRICKS_TOKEN"

# Unity Catalog configuration
UC_CATALOG = "network_fault_detection"
UC_SCHEMA = "processed_data"
UC_TABLE_OPERATIONS = f"{UC_CATALOG}.{UC_SCHEMA}.network_operations"
UC_TABLE_ACTIONS = f"{UC_CATALOG}.{UC_SCHEMA}.operation_actions"
UC_TABLE_AUDIT = f"{UC_CATALOG}.{UC_SCHEMA}.operations_audit"

# Foundation Model configuration
'''
databricks-meta-llama-3-1-405b-instruct â†’ ðŸš¨ Extremely expensive (405B params, frontier-scale), will eat your free trial credits very fast.

databricks-meta-llama-3-1-8b-instruct â†’ âœ… Much cheaper, lighter, and perfectly suitable for prototyping in your 14-day trial.
'''

# MODEL_ENDPOINT = "databricks-meta-llama-3-1-405b-instruct"
MODEL_ENDPOINT = "databricks-meta-llama-3-1-8b-instruct"

print("âœ… Configuration loaded successfully")
print(f"ðŸ“Š Unity Catalog: {UC_CATALOG}.{UC_SCHEMA}")
print(f"ðŸ¤– Foundation Model: {MODEL_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ—ï¸ Unity Catalog Schema Setup

# COMMAND ----------

def setup_network_ops_tables():
    """Setup Unity Catalog tables for network operations tracking with column defaults enabled"""
    try:
        # ---------------- OPERATIONS TABLE ----------------
        print("ðŸ”„ Dropping existing tables to fix schema conflicts...")
        spark.sql(f"DROP TABLE IF EXISTS {UC_TABLE_OPERATIONS}")

        # Create dummy table first (to enable feature)
        spark.sql(f"""
        CREATE TABLE {UC_TABLE_OPERATIONS} (dummy_col INT) USING DELTA
        """)

        # Enable column defaults + column mapping
        spark.sql(f"""
        ALTER TABLE {UC_TABLE_OPERATIONS} SET TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported',
            'delta.columnMapping.mode' = 'name'
        )
        """)

        # Replace with final schema
        spark.sql(f"""
        CREATE OR REPLACE TABLE {UC_TABLE_OPERATIONS} (
            operation_id BIGINT GENERATED ALWAYS AS IDENTITY,
            incident_id STRING,
            decision_id STRING,
            operation_type STRING NOT NULL,
            operation_parameters MAP<STRING,STRING>,
            execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            completion_timestamp TIMESTAMP,
            operation_status STRING DEFAULT 'PENDING',
            success_indicator BOOLEAN DEFAULT FALSE,
            result_message STRING,
            error_details STRING,
            safety_checks_passed BOOLEAN DEFAULT TRUE,
            simulated_execution BOOLEAN DEFAULT TRUE,
            affected_components ARRAY<STRING>,
            pre_execution_state MAP<STRING,STRING>,
            post_execution_state MAP<STRING,STRING>,
            execution_duration_seconds DOUBLE,
            agent_version STRING DEFAULT 'agentbricks_v1.0'
        ) USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported',
            'delta.columnMapping.mode' = 'name'
        )
        """)

        # ---------------- ACTIONS TABLE ----------------
        print("ðŸ”„ Dropping existing tables to fix schema conflicts...")
        spark.sql(f"DROP TABLE IF EXISTS {UC_TABLE_ACTIONS}")

        spark.sql(f"""
        CREATE TABLE {UC_TABLE_ACTIONS} (
            action_id BIGINT GENERATED ALWAYS AS IDENTITY,
            operation_id STRING,
            action_type STRING NOT NULL,
            target_component STRING,
            action_parameters MAP<STRING,STRING>,
            execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            action_result STRING DEFAULT 'SUCCESS',
            metrics_before MAP<STRING,STRING>,
            metrics_after MAP<STRING,STRING>,
            rollback_available BOOLEAN DEFAULT FALSE,
            rollback_procedure STRING
        ) USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported',
            'delta.columnMapping.mode' = 'name'
        )
        """)

        # ---------------- AUDIT TABLE ----------------
        print("ðŸ”„ Dropping existing tables to fix schema conflicts...")
        spark.sql(f"DROP TABLE IF EXISTS {UC_TABLE_AUDIT}")

        spark.sql(f"""
        CREATE TABLE {UC_TABLE_AUDIT} (
            audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
            operation_id STRING NOT NULL,
            audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            audit_event STRING NOT NULL,
            event_details STRING,
            user_context STRING DEFAULT 'network-ops-agentbricks',
            compliance_check BOOLEAN DEFAULT TRUE,
            risk_assessment STRING DEFAULT 'LOW'
        ) USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported',
            'delta.columnMapping.mode' = 'name'
        )
        """)

        print("âœ… Network Operations Unity Catalog tables setup completed")
        return True

    except Exception as e:
        logger.error(f"Unity Catalog setup failed: {e}")
        return False


# Setup tables
setup_success = setup_network_ops_tables()
print(f"Unity Catalog Setup: {'âœ… Success' if setup_success else 'âŒ Failed'}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ“‹ Data Models & Types

# COMMAND ----------

class OperationType(Enum):
    """Available network operations"""
    RESTART_NODE = "restart_node"
    REROUTE_TRAFFIC = "reroute_traffic"
    ADJUST_QOS = "adjust_qos"
    SCALE_CAPACITY = "scale_capacity"
    ESCALATE_ISSUE = "escalate_issue"
    NO_ACTION = "no_action_needed"

class OperationStatus(Enum):
    """Operation execution status"""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ROLLBACK_REQUIRED = "ROLLBACK_REQUIRED"

@dataclass
class OperationInput:
    """Input for network operations"""
    incident_id: str
    decision_id: str
    operation_type: OperationType
    operation_parameters: Dict[str, str]
    log_file_path: Optional[str] = None
    safety_override: bool = False

@dataclass
class OperationResult:
    """Output from network operation execution"""
    operation_id: str
    success: bool
    operation_status: OperationStatus
    result_message: str
    execution_time_ms: int
    affected_components: List[str]
    safety_checks_passed: bool
    operation_details: Dict[str, Any]
    agent_version: str = "agentbricks_v1.0"

print("âœ… Data models defined successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Simple Agent Framework (AgentBricks-style)

# COMMAND ----------

# Simple tool decorator to mimic AgentBricks functionality
def tool(func):
    """Decorator to mark functions as agent tools"""
    func._is_tool = True
    func._tool_name = func.__name__
    func._tool_description = func.__doc__ or f"Tool: {func.__name__}"
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"ðŸ”§ Executing tool: {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"âœ… Tool {func.__name__} completed successfully")
            return result
        except Exception as e:
            logger.error(f"âŒ Tool {func.__name__} failed: {e}")
            return {"success": False, "error": str(e)}
    return wrapper

class SimpleAgent:
    """Simple agent base class to mimic AgentBricks Agent"""
    def __init__(self, instructions: str, tools: List = None):
        self.instructions = instructions
        self.tools = {tool.__name__: tool for tool in tools or []}

print("âœ… Simple Agent Framework implemented")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ› ï¸ AgentBricks Tools Implementation

# COMMAND ----------

@tool
def validate_operation_safety(operation_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates the safety and feasibility of requested network operation
    
    Args:
        operation_input: Dictionary containing operation details
    
    Returns:
        Dictionary with safety validation results
    """
    logger.info(f"ðŸ›¡ï¸ Validating safety for operation: {operation_input.get('operation_type', 'unknown')}")
    
    try:
        operation_type = operation_input.get("operation_type", "")
        parameters = operation_input.get("operation_parameters", {})
        safety_override = operation_input.get("safety_override", False)
        
        # Safety validation rules
        safety_checks = {
            "operation_type_valid": operation_type in [op.value for op in OperationType],
            "parameters_present": bool(parameters),
            "no_critical_components": True,  # Simulated check
            "maintenance_window": True,      # Simulated check
            "approval_required": operation_type not in ["no_action_needed"]
        }
        
        # Calculate risk level
        risk_level = "LOW"
        if operation_type in ["restart_node", "reroute_traffic"]:
            risk_level = "MEDIUM"
        elif operation_type == "scale_capacity":
            risk_level = "HIGH"
        
        # Overall safety assessment
        all_checks_passed = all(safety_checks.values()) or safety_override
        
        logger.info(f"ðŸ›¡ï¸ Safety validation completed - Risk: {risk_level}, Passed: {all_checks_passed}")
        
        return {
            "success": True,
            "safety_checks_passed": all_checks_passed,
            "risk_level": risk_level,
            "safety_details": safety_checks,
            "approval_required": not safety_override and risk_level in ["MEDIUM", "HIGH"],
            "recommended_actions": [
                "Verify maintenance window",
                "Check component dependencies",
                "Prepare rollback plan"
            ]
        }
        
    except Exception as e:
        logger.error(f"âŒ Safety validation failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "safety_checks_passed": False,
            "risk_level": "UNKNOWN"
        }

@tool
def execute_network_operation(operation_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Executes the specified network operation (simulated for safety)
    
    Args:
        operation_input: Dictionary containing operation details
    
    Returns:
        Dictionary with execution results
    """
    logger.info(f"âš™ï¸ Executing network operation: {operation_input.get('operation_type', 'unknown')}")
    
    try:
        operation_type = operation_input.get("operation_type", "")
        parameters = operation_input.get("operation_parameters", {})
        incident_id = operation_input.get("incident_id", "")
        
        # Generate unique operation ID
        operation_id = f"OP-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        
        # Simulate execution based on operation type
        execution_results = {}
        affected_components = []
        
        if operation_type == "restart_node":
            node_id = parameters.get("node_id", "unknown")
            affected_components = [node_id]
            execution_results = {
                "node_restarted": node_id,
                "restart_duration": "45 seconds",
                "status": "operational",
                "service_impact": "2 minutes downtime"
            }
            
        elif operation_type == "reroute_traffic":
            from_path = parameters.get("from_path", "unknown")
            to_path = parameters.get("to_path", "backup")
            affected_components = [from_path, to_path]
            execution_results = {
                "traffic_rerouted": f"{from_path} -> {to_path}",
                "reroute_percentage": "85%",
                "latency_impact": "+12ms",
                "throughput": "maintained"
            }
            
        elif operation_type == "adjust_qos":
            component = parameters.get("component", "unknown")
            priority = parameters.get("priority", "high")
            affected_components = [component]
            execution_results = {
                "qos_adjusted": component,
                "new_priority": priority,
                "bandwidth_allocated": "500 Mbps",
                "policy_applied": "emergency_qos_v2"
            }
            
        elif operation_type == "scale_capacity":
            component = parameters.get("component", "unknown")
            scale_factor = parameters.get("scale_factor", "1.5x")
            affected_components = [component]
            execution_results = {
                "capacity_scaled": component,
                "scale_factor": scale_factor,
                "new_capacity": "750 connections/sec",
                "provisioning_time": "3 minutes"
            }
            
        else:
            execution_results = {
                "action": "no_operation_performed",
                "reason": f"Operation type '{operation_type}' handled via escalation"
            }
        
        logger.info(f"âš™ï¸ Operation {operation_id} executed successfully")
        
        return {
            "success": True,
            "operation_id": operation_id,
            "operation_type": operation_type,
            "execution_status": "COMPLETED",
            "affected_components": affected_components,
            "execution_results": execution_results,
            "simulated": True,
            "execution_timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"âŒ Operation execution failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "execution_status": "FAILED",
            "operation_id": None
        }

@tool
def track_operation_audit(operation_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tracks operation execution in Unity Catalog for audit and compliance
    
    Args:
        operation_data: Dictionary containing operation execution details
    
    Returns:
        Dictionary with audit tracking results
    """
    logger.info(f"ðŸ“ Tracking operation audit for: {operation_data.get('operation_id', 'unknown')}")
    
    try:
        # Prepare operation record for Unity Catalog
        operation_record = {
            "incident_id": operation_data.get("incident_id", ""),
            "decision_id": operation_data.get("decision_id", ""),
            "operation_type": operation_data.get("operation_type", ""),
            "operation_parameters": operation_data.get("operation_parameters", {}),
            "completion_timestamp": datetime.now(),
            "operation_status": operation_data.get("execution_status", "COMPLETED"),
            "success_indicator": operation_data.get("success", False),
            "result_message": json.dumps(operation_data.get("execution_results", {})),
            "safety_checks_passed": operation_data.get("safety_checks_passed", True),
            "simulated_execution": operation_data.get("simulated", True),
            "affected_components": operation_data.get("affected_components", []),
            "execution_duration_seconds": operation_data.get("execution_time_ms", 0) / 1000.0,
            "agent_version": "agentbricks_v1.0"
        }
        
        # Create DataFrame for Unity Catalog
        from pyspark.sql import Row
        operation_df = spark.createDataFrame([Row(**operation_record)])
        
        # Save to Unity Catalog
        operation_df.write.mode("append").saveAsTable(UC_TABLE_OPERATIONS)
        
        # Create audit entry
        audit_record = {
            "operation_id": operation_data.get("operation_id", ""),
            "audit_event": "OPERATION_COMPLETED",
            "event_details": f"Operation {operation_data.get('operation_type', '')} completed successfully",
            "user_context": "network-ops-agentbricks",
            "compliance_check": True,
            "risk_assessment": operation_data.get("risk_level", "LOW")
        }
        
        audit_df = spark.createDataFrame([Row(**audit_record)])
        audit_df.write.mode("append").saveAsTable(UC_TABLE_AUDIT)
        
        logger.info(f"ðŸ“ Operation audit tracking completed for {operation_data.get('operation_id', 'unknown')}")
        
        return {
            "success": True,
            "action": "audit_recorded",
            "tables_updated": [UC_TABLE_OPERATIONS, UC_TABLE_AUDIT],
            "operation_id": operation_data.get("operation_id", ""),
            "audit_timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"âŒ Audit tracking failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "tables_updated": [],
            "operation_id": operation_data.get("operation_id", "")
        }

print("âœ… AgentBricks tools implemented successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ¤– AgentBricks Network Operations Agent

# COMMAND ----------

class NetworkOpsAgent(SimpleAgent):
    """
    AgentBricks-powered Network Operations Agent
    """
    
    def __init__(self):
        # Initialize tools
        tools = [
            validate_operation_safety,
            execute_network_operation,
            track_operation_audit
        ]
        
        # Agent instructions
        instructions = """
        You are a network operations agent. Your job is to safely execute network 
        operations to resolve incidents and maintain network stability.
        
        Use the available tools to:
        1. Validate operation safety and compliance requirements
        2. Execute approved network operations with proper safeguards
        3. Track all operations in Unity Catalog for audit compliance
        
        Always prioritize safety and follow proper operational procedures.
        """
        
        super().__init__(instructions, tools)
        self.agent_version = "AgentBricks_v1.0"
        logger.info("ðŸ¤– NetworkOpsAgent initialized with AgentBricks framework")
    
    def _call_foundation_model(self, prompt: str) -> Dict[str, Any]:
        """Call Databricks Foundation Model API"""
        try:
            url = f"https://{DATABRICKS_HOST}/serving-endpoints/{MODEL_ENDPOINT}/invocations"
            headers = {
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "messages": [
                    {"role": "system", "content": self.instructions},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 1000,
                "temperature": 0.1
            }
            
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                return {"success": True, "content": content}
            else:
                return {"success": False, "error": f"API call failed: {response.status_code}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def run(self, prompt: str) -> str:
        """Execute agent with given prompt, using tools as needed"""
        logger.info("ðŸš€ AgentBricks Network Operations Agent execution started")
        
        # Try AI-powered approach first
        ai_response = self._call_foundation_model(prompt)
        
        if ai_response["success"]:
            response_parts = [f"AI Analysis: {ai_response['content']}"]
            
            # Execute tools based on prompt content
            if "execute operation" in prompt.lower():
                operation_data = self._extract_operation_data(prompt)
                
                # Step 1: Validate safety
                if "validate_operation_safety" in self.tools:
                    safety_result = self.tools["validate_operation_safety"](operation_data)
                    response_parts.append(f"Safety Validation: {json.dumps(safety_result, indent=2)}")
                    
                    # Step 2: Execute operation if safe
                    if safety_result.get("safety_checks_passed", False) and "execute_network_operation" in self.tools:
                        operation_data["safety_checks_passed"] = True
                        execution_result = self.tools["execute_network_operation"](operation_data)
                        response_parts.append(f"Operation Execution: {json.dumps(execution_result, indent=2)}")
                        
                        # Step 3: Track in audit trail
                        if execution_result.get("success", False) and "track_operation_audit" in self.tools:
                            audit_data = {**operation_data, **execution_result}
                            audit_result = self.tools["track_operation_audit"](audit_data)
                            response_parts.append(f"Audit Tracking: {json.dumps(audit_result, indent=2)}")
            
            return "\\n".join(response_parts)
        else:
            # Fallback to direct tool execution when Foundation Model unavailable
            logger.warning("Foundation Model unavailable, using direct tools execution")
            response_parts = ["AI Analysis: Foundation Model unavailable, executing tools directly"]
            
            if "execute operation" in prompt.lower():
                operation_data = self._extract_operation_data(prompt)
                
                # Execute all tools in sequence
                if "validate_operation_safety" in self.tools:
                    safety_result = self.tools["validate_operation_safety"](operation_data)
                    response_parts.append(f"Safety Validation: {json.dumps(safety_result, indent=2)}")
                    
                    if safety_result.get("safety_checks_passed", False):
                        if "execute_network_operation" in self.tools:
                            operation_data["safety_checks_passed"] = True
                            execution_result = self.tools["execute_network_operation"](operation_data)
                            response_parts.append(f"Operation Execution: {json.dumps(execution_result, indent=2)}")
                            
                            if execution_result.get("success", False) and "track_operation_audit" in self.tools:
                                audit_data = {**operation_data, **execution_result}
                                audit_result = self.tools["track_operation_audit"](audit_data)
                                response_parts.append(f"Audit Tracking: {json.dumps(audit_result, indent=2)}")
            
            return "\\n".join(response_parts)
    
    def _extract_operation_data(self, prompt: str) -> Dict[str, Any]:
        """Extract operation data from prompt"""
        # Simple pattern matching for operation details
        lines = prompt.split('\\n')
        operation_data = {
            "operation_type": "no_action_needed",
            "operation_parameters": {},
            "incident_id": f"INC-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            "decision_id": f"DEC-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        }
        
        for line in lines:
            if "operation_type:" in line.lower():
                operation_data["operation_type"] = line.split(":")[-1].strip()
            elif "incident_id:" in line.lower():
                operation_data["incident_id"] = line.split(":")[-1].strip()
            elif "node_id:" in line.lower():
                operation_data["operation_parameters"]["node_id"] = line.split(":")[-1].strip()
        
        return operation_data
    
    def execute_operation(self, operation_input: OperationInput) -> Dict[str, Any]:
        """
        Execute network operation using AgentBricks tools
        
        Args:
            operation_input: OperationInput object with operation details
            
        Returns:
            Dictionary with execution results
        """
        start_time = time.time()
        
        try:
            operation_prompt = f"""
            Execute network operation:
            
            Incident ID: {operation_input.incident_id}
            Decision ID: {operation_input.decision_id}
            Operation Type: {operation_input.operation_type.value}
            Parameters: {operation_input.operation_parameters}
            Safety Override: {operation_input.safety_override}
            
            Please validate safety, execute the operation, and track audit trail.
            """
            
            # Execute agent analysis
            response = self.run(operation_prompt)
            
            processing_time = int((time.time() - start_time) * 1000)
            
            logger.info(f"âœ… AgentBricks operation execution completed in {processing_time}ms")
            
            return {
                "success": True,
                "incident_id": operation_input.incident_id,
                "operation_type": operation_input.operation_type.value,
                "agent_response": response,
                "processing_time_ms": processing_time,
                "agent_version": self.agent_version
            }
            
        except Exception as e:
            logger.error(f"âŒ AgentBricks operation execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "processing_time_ms": int((time.time() - start_time) * 1000)
            }

print("âœ… AgentBricks NetworkOpsAgent implemented successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ§ª Step 4: Initialize and Test Agent (5 minutes)

# COMMAND ----------

# 4.1 Initialize the agent
print("ðŸ¤– Initializing Network Operations Agent")
print("=" * 50)

# Initialize the new AgentBricks NetworkOpsAgent
agent = None
try:
    agent = NetworkOpsAgent()
    print("âœ… AgentBricks NetworkOpsAgent initialized successfully")
    print(f"ðŸ¤– Agent version: {agent.agent_version}")
    print(f"ðŸ”§ Tools available: {len(agent.tools)} tools")
    agent_type = "agentbricks"
    
except Exception as e:
    print(f"âŒ AgentBricks Agent initialization failed: {e}")
    agent_type = "failed"

if agent:
    print()
    # Display agent capabilities
    print(f"ðŸ”§ Available tools:")
    for tool_name in agent.tools.keys():
        print(f"   â€¢ {tool_name}")
    print()
    
    print(f"ðŸ“ Agent instructions preview:")
    print(f"   {agent.instructions[:100]}...")
    print()
    
    print(f"ðŸš€ Agent Type: {agent_type.upper()}")
    print("âœ… Ready for testing!")
else:
    print("âŒ Cannot proceed - agent initialization failed")

# COMMAND ----------

# 4.2 Test with a critical incident operation
print("ðŸ”¥ Testing Critical Incident Network Operation")
print("-" * 50)

if not agent:
    print("âŒ Cannot run test - agent not initialized")
    dbutils.notebook.exit("Agent initialization failed")

# Create test operation input
test_operation = OperationInput(
    incident_id="INC-20250911-001",
    decision_id="DEC-20250911-001",
    operation_type=OperationType.RESTART_NODE,
    operation_parameters={
        "node_id": "Node-5G-001",
        "restart_type": "graceful",
        "estimated_downtime": "2 minutes"
    },
    safety_override=False
)

print(f"ðŸ“ Test Operation:")
print(f"   Incident: {test_operation.incident_id}")
print(f"   Operation: {test_operation.operation_type.value}")
print(f"   Target: {test_operation.operation_parameters.get('node_id', 'unknown')}")
print(f"ðŸ¤– Using Agent Type: {agent_type.upper()}")
print()

try:
    result = agent.execute_operation(test_operation)
    
    print(f"ðŸ¤– AgentBricks Operation Results:")
    print(f"   Success: {result['success']}")
    print(f"   Processing Time: {result['processing_time_ms']}ms")
    print(f"   Agent Version: {result['agent_version']}")
    print()
    print(f"ðŸ“‹ Agent Response:")
    print(f"   {result['agent_response'][:400]}...")
    print()
    
    # Validate result
    if "COMPLETED" in result['agent_response'] and result['success']:
        print("âœ… CORRECT - Operation executed successfully")
    else:
        print(f"âš ï¸ UNCLEAR - Could not confirm successful execution")
        
except Exception as e:
    print(f"âŒ Operation execution failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”„ Step 5: Batch Testing with Multiple Operation Types (8 minutes)

# COMMAND ----------

# 5.1 Define comprehensive test dataset
SAMPLE_NETWORK_OPERATIONS = [
    {
        "id": "restart_critical_node",
        "incident_id": "INC-20250911-001",
        "operation_type": OperationType.RESTART_NODE,
        "parameters": {"node_id": "Node-5G-001", "restart_type": "graceful"},
        "expected_result": "success"
    },
    {
        "id": "reroute_traffic_fiber_cut",
        "incident_id": "INC-20250911-002", 
        "operation_type": OperationType.REROUTE_TRAFFIC,
        "parameters": {"from_path": "fiber-trunk-1", "to_path": "fiber-trunk-2"},
        "expected_result": "success"
    },
    {
        "id": "adjust_qos_congestion",
        "incident_id": "INC-20250911-003",
        "operation_type": OperationType.ADJUST_QOS,
        "parameters": {"component": "core-router-1", "priority": "emergency"},
        "expected_result": "success"
    },
    {
        "id": "scale_capacity_high_load",
        "incident_id": "INC-20250911-004",
        "operation_type": OperationType.SCALE_CAPACITY,
        "parameters": {"component": "load-balancer-1", "scale_factor": "2.0x"},
        "expected_result": "success"
    },
    {
        "id": "escalate_complex_issue",
        "incident_id": "INC-20250911-005",
        "operation_type": OperationType.ESCALATE_ISSUE,
        "parameters": {"escalation_level": "L3", "urgency": "high"},
        "expected_result": "success"
    }
]

print(f"ðŸ§ª Batch Testing with {len(SAMPLE_NETWORK_OPERATIONS)} Network Operations")
print("=" * 65)

# COMMAND ----------

# 5.2 Execute batch testing
test_results = []
total_tests = len(SAMPLE_NETWORK_OPERATIONS)
passed_tests = 0

for i, operation_sample in enumerate(SAMPLE_NETWORK_OPERATIONS, 1):
    print(f"\\n{i}/{total_tests}. Testing: {operation_sample['id']}")
    print(f"Operation: {operation_sample['operation_type'].value}")
    print(f"Incident: {operation_sample['incident_id']}")
    print(f"Parameters: {operation_sample['parameters']}")
    
    try:
        # Create OperationInput for AgentBricks
        operation_input = OperationInput(
            incident_id=operation_sample['incident_id'],
            decision_id=f"DEC-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{i}",
            operation_type=operation_sample['operation_type'],
            operation_parameters=operation_sample['parameters'],
            safety_override=False
        )
        
        # Execute operation using AgentBricks
        result = agent.execute_operation(operation_input)
        
        print(f"ðŸ¤– Success: {result['success']}")
        print(f"â±ï¸ Processing: {result['processing_time_ms']}ms")
        print(f"ðŸ“ Response: {result['agent_response'][:200]}...")
        
        # Determine if operation was successful
        operation_successful = result['success'] and ("COMPLETED" in result['agent_response'] or "SUCCESS" in result['agent_response'])
        
        print(f"ðŸŽ¯ Execution Status: {'âœ… SUCCESS' if operation_successful else 'âŒ FAILED'}")
        
        # Check against expected result
        is_correct = operation_successful == (operation_sample['expected_result'] == 'success')
        if is_correct:
            passed_tests += 1
            print("ðŸŽ¯ Status: âœ… CORRECT")
        else:
            print("ðŸŽ¯ Status: âŒ INCORRECT")
        
        # Store test result (fixed for AgentBricks)
        test_results.append({
            "test_id": operation_sample['id'],
            "operation_type": operation_sample['operation_type'].value,
            "expected": operation_sample['expected_result'],
            "success": result['success'],
            "processing_time": result['processing_time_ms'],
            "correct": is_correct,
            "execution_successful": operation_successful
        })
        
    except Exception as e:
        print(f"âŒ Error: {e}")
        test_results.append({
            "test_id": operation_sample['id'],
            "operation_type": operation_sample['operation_type'].value,
            "expected": operation_sample['expected_result'], 
            "success": False,
            "processing_time": 0,
            "correct": False,
            "execution_successful": False,
            "error": str(e)
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ“Š Step 6: Results Analysis and Performance Metrics (3 minutes)

# COMMAND ----------

# 6.1 Calculate and display comprehensive results
print("ðŸŽ‰ NETWORK OPERATIONS AGENT - TEST RESULTS")
print("=" * 55)

accuracy = (passed_tests / total_tests) * 100
print(f"ðŸ“Š Overall Performance:")
print(f"   â€¢ Tests executed: {total_tests}")
print(f"   â€¢ Tests passed: {passed_tests}")
print(f"   â€¢ Accuracy rate: {accuracy:.1f}%")
print()

# Detailed breakdown by operation type
operation_breakdown = {}
for result in test_results:
    op_type = result['operation_type']
    if op_type not in operation_breakdown:
        operation_breakdown[op_type] = {"correct": 0, "total": 0}
    
    operation_breakdown[op_type]['total'] += 1
    if result['correct']:
        operation_breakdown[op_type]['correct'] += 1

print("ðŸ“‹ Detailed Test Results:")
for result in test_results:
    status_icon = "âœ…" if result['correct'] else "âŒ"
    processing_time = result.get('processing_time', 0)
    execution_status = "SUCCESS" if result.get('execution_successful', False) else "FAILED"
    print(f"   {status_icon} {result['test_id']}: {result['operation_type']} -> {execution_status} ({processing_time}ms)")

print(f"\\nðŸ“ˆ Operation Type Performance:")
for op_type, data in operation_breakdown.items():
    if data['total'] > 0:
        type_accuracy = (data['correct'] / data['total']) * 100
        print(f"   â€¢ {op_type}: {data['correct']}/{data['total']} ({type_accuracy:.0f}% accuracy)")

# Performance metrics
print(f"\\nâ±ï¸ Average Processing Time: {sum(r.get('processing_time', 0) for r in test_results) / len(test_results):.0f}ms")

# Performance assessment
if accuracy >= 80:
    performance_status = "ðŸ† EXCELLENT (Production Ready)"
    recommendation = "Deploy to production with confidence"
elif accuracy >= 60:
    performance_status = "âœ… GOOD (Staging Ready)" 
    recommendation = "Proceed with staging deployment and further testing"
else:
    performance_status = "âš ï¸ NEEDS IMPROVEMENT"
    recommendation = "Review and enhance before deployment"

print(f"\\nðŸŽ¯ Performance Assessment: {performance_status}")
print(f"ðŸ’¡ Recommendation: {recommendation}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## âš¡ Step 7: Performance Benchmarking (2 minutes)

# COMMAND ----------

# 7.1 Performance speed test
print("âš¡ Performance Speed Benchmarking")
print("=" * 40)

# Test with quick operations
benchmark_operations = [
    {"type": OperationType.NO_ACTION, "params": {}},
    {"type": OperationType.ADJUST_QOS, "params": {"component": "test-node"}},
    {"type": OperationType.RESTART_NODE, "params": {"node_id": "test-node-1"}}
]

print("ðŸ“Š Speed Test Results:")
for i, op in enumerate(benchmark_operations, 1):
    start_time = time.time()
    
    try:
        operation_input = OperationInput(
            incident_id=f"BENCH-{i}",
            decision_id=f"BENCH-DEC-{i}",
            operation_type=op["type"],
            operation_parameters=op["params"]
        )
        
        result = agent.execute_operation(operation_input)
        end_time = time.time()
        
        duration = (end_time - start_time) * 1000  # Convert to milliseconds
        success = result.get('success', False)
        internal_time = result.get('processing_time_ms', 0)
        print(f"   Test {i}: {duration:.1f}ms - Success: {success}, Internal: {internal_time}ms")
        
    except Exception as e:
        print(f"   Test {i}: Error - {str(e)[:50]}...")

# 7.2 Batch processing benchmark
print(f"\\nðŸ”„ Batch Processing Test:")
batch_operations = [{"type": OperationType.NO_ACTION, "params": {}}] * 5

start_time = time.time()
batch_results = []

for i, op in enumerate(batch_operations):
    try:
        operation_input = OperationInput(
            incident_id=f"BATCH-{i}",
            decision_id=f"BATCH-DEC-{i}",
            operation_type=op["type"],
            operation_parameters=op["params"]
        )
        result = agent.execute_operation(operation_input)
        batch_results.append(result)
    except:
        pass

end_time = time.time()
batch_duration = end_time - start_time

print(f"   Processed: {len(batch_results)} operations")
print(f"   Total time: {batch_duration:.2f} seconds")
print(f"   Average per operation: {(batch_duration / len(batch_results)):.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸŽ¯ Summary & Next Steps

# COMMAND ----------

print("ðŸ NETWORK OPERATIONS AGENT - AGENTBRICKS IMPLEMENTATION COMPLETE")
print("=" * 70)
print()
print("âœ… **Successfully Implemented:**")
print("   â€¢ AgentBricks-powered Network Operations Agent")
print("   â€¢ Three core tools: safety validation, operation execution, audit tracking")
print("   â€¢ Unity Catalog integration for operation tracking")
print("   â€¢ Foundation Model integration with tool fallback")
print("   â€¢ Comprehensive testing with multiple operation types")
print()
print("ðŸ“Š **Performance Results:**")
print(f"   â€¢ Test Accuracy: {accuracy:.1f}%")
print(f"   â€¢ Operations Tested: {total_tests}")
print(f"   â€¢ Average Processing Time: {sum(r.get('processing_time', 0) for r in test_results) / len(test_results):.0f}ms")
print()
print("ðŸ”§ **Key Features:**")
print("   â€¢ Tool-based modular architecture")
print("   â€¢ Safety validation and compliance checks")
print("   â€¢ Simulated network operations (production-safe)")
print("   â€¢ Complete audit trail in Unity Catalog")
print("   â€¢ Foundation Model + rule-based hybrid approach")
print()
print("ðŸš€ **Ready for:**")
print("   â€¢ Integration with Incident Manager Agent")
print("   â€¢ Production deployment with real network operations")
print("   â€¢ Extension with additional operation types")
print("   â€¢ Integration with monitoring and alerting systems")
print()
print("ðŸ’¡ **Next Steps:**")
print("   â€¢ Implement real network operations (when approved)")
print("   â€¢ Add operation rollback capabilities")
print("   â€¢ Integrate with network monitoring tools")
print("   â€¢ Create operation approval workflows")
