# Databricks notebook source
# MAGIC %md
# MAGIC # Incident Manager Agent - Databricks Implementation (FIXED API)
# MAGIC
# MAGIC ## 🎯 Overview
# MAGIC This notebook implements the **Network Incident Manager Agent** that:
# MAGIC - Reads network log files and analyzes incidents
# MAGIC - Makes operational decisions based on incident severity and patterns
# MAGIC - Coordinates with Severity Classifier and Network Operations agents
# MAGIC - Integrates with Unity Catalog for enterprise governance
# MAGIC - Provides intelligent incident management and decision-making
# MAGIC
# MAGIC ## 🏗️ Architecture
# MAGIC ```
# MAGIC Input: Network Log Files + Severity Classification
# MAGIC ↓
# MAGIC Log Analysis & Pattern Recognition
# MAGIC ↓
# MAGIC Decision Engine (Foundation Model + Rules)
# MAGIC ↓
# MAGIC Action Recommendations: Restart | Reroute | Scale | Escalate
# MAGIC ↓
# MAGIC Unity Catalog Logging & Orchestration
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Dependencies Installation

# COMMAND ----------

# Install required packages
%pip install databricks-sdk mlflow python-dotenv requests numpy pandas

# Restart Python to ensure packages are loaded
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration & Setup

# COMMAND ----------

import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
import re
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Databricks configuration
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', dbutils.secrets.get('default', 'databricks-host'))
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', dbutils.secrets.get('default', 'databricks-token'))

# Unity Catalog configuration
UC_CATALOG = "network_fault_detection"
UC_SCHEMA = "processed_data"
UC_TABLE_INCIDENTS = "network_fault_detection.processed_data.incident_decisions"
UC_TABLE_ACTIONS = "network_fault_detection.processed_data.network_actions"

# Model configuration (MATCHING working Severity Classification agent)
INCIDENT_MODEL_ENDPOINT = "databricks-meta-llama-3-1-405b-instruct"

print("🚀 Incident Manager Agent Configuration:")
print(f"   • Unity Catalog: {UC_CATALOG}")
print(f"   • Incidents Table: {UC_TABLE_INCIDENTS}")
print(f"   • Model Endpoint: {INCIDENT_MODEL_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗄️ Unity Catalog Schema Setup

# COMMAND ----------

def setup_incident_manager_tables():
    """Initialize Unity Catalog tables for incident management - drops existing tables to fix schema conflicts"""
    
    try:
        # Drop existing tables to fix schema conflicts
        print("🔄 Dropping existing tables to fix schema conflicts...")
        spark.sql(f"DROP TABLE IF EXISTS {UC_TABLE_INCIDENTS}")
        spark.sql(f"DROP TABLE IF EXISTS {UC_TABLE_ACTIONS}")
        
        # Create incident decisions table with correct schema
        print("📋 Creating incident decisions table...")
        spark.sql(f"""
        CREATE TABLE {UC_TABLE_INCIDENTS} (
            decision_id BIGINT GENERATED ALWAYS AS IDENTITY,
            incident_id STRING NOT NULL,
            log_file_path STRING,
            severity_classification STRING NOT NULL,
            decision_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            recommended_action STRING NOT NULL,
            action_parameters MAP<STRING, STRING>,
            confidence_score DOUBLE DEFAULT 0.0,
            reasoning STRING,
            affected_components ARRAY<STRING>,
            estimated_impact_users BIGINT DEFAULT 0,
            priority_level STRING DEFAULT 'STANDARD',
            escalation_required BOOLEAN DEFAULT FALSE,
            agent_version STRING DEFAULT 'v1.0'
        ) USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported',
            'delta.columnMapping.mode' = 'name'
        )
        """)
        
        # Create network actions tracking table
        print("🔧 Creating network actions table...")
        spark.sql(f"""
        CREATE TABLE {UC_TABLE_ACTIONS} (
            action_id BIGINT GENERATED ALWAYS AS IDENTITY,
            incident_id STRING NOT NULL,
            decision_id BIGINT,
            action_type STRING NOT NULL,
            action_status STRING DEFAULT 'PENDING',
            execution_timestamp TIMESTAMP,
            completion_timestamp TIMESTAMP,
            action_result STRING,
            error_message STRING,
            executed_by STRING DEFAULT 'network-ops-agent'
        ) USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported'
        )
        """)
        
        print("✅ Incident Manager Unity Catalog tables setup completed")
        return True
        
    except Exception as e:
        logger.error(f"Unity Catalog setup failed: {e}")
        return False

# Setup tables
setup_success = setup_incident_manager_tables()
print(f"Unity Catalog Setup: {'✅ Success' if setup_success else '❌ Failed'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Incident Manager Core Implementation

# COMMAND ----------

class ActionType(Enum):
    """Available network operations actions"""
    RESTART_NODE = "restart_node"
    REROUTE_TRAFFIC = "reroute_traffic" 
    ADJUST_QOS = "adjust_qos"
    SCALE_CAPACITY = "scale_capacity"
    NO_ACTION = "no_action_needed"
    ESCALATE_ISSUE = "escalate_issue"

class PriorityLevel(Enum):
    """Incident priority levels"""
    EMERGENCY = "EMERGENCY"
    HIGH = "HIGH" 
    STANDARD = "STANDARD"
    LOW = "LOW"

@dataclass
class IncidentInput:
    """Input data for incident management"""
    incident_id: str
    log_file_path: str
    log_content: str
    severity_classification: str
    detected_timestamp: datetime
    additional_context: Optional[Dict] = None

@dataclass
class IncidentDecision:
    """Output decision from incident manager"""
    decision_id: str
    incident_id: str
    recommended_action: ActionType
    action_parameters: Dict[str, str]
    confidence_score: float
    reasoning: str
    affected_components: List[str]
    estimated_impact_users: int
    priority_level: PriorityLevel
    escalation_required: bool

class DatabricksIncidentManager:
    """
    Network Incident Manager Agent for Databricks
    Analyzes incidents and makes operational decisions
    """
    
    def __init__(self, model_endpoint: str = INCIDENT_MODEL_ENDPOINT):
        self.model_endpoint = model_endpoint
        self.agent_version = "v1.0"
        
        # Decision rules mapping
        self.action_rules = {
            # Node/Infrastructure failures
            "node_down": ActionType.RESTART_NODE,
            "heartbeat_missed": ActionType.RESTART_NODE,
            "radio_process_crash": ActionType.RESTART_NODE,
            
            # Traffic/Congestion issues
            "severe_congestion": ActionType.REROUTE_TRAFFIC,
            "cell_overload": ActionType.REROUTE_TRAFFIC,
            
            # Quality of Service issues
            "packet_loss": ActionType.ADJUST_QOS,
            "high_jitter": ActionType.ADJUST_QOS,
            "voice_quality_degraded": ActionType.ADJUST_QOS,
            
            # Capacity issues
            "prb_utilization_high": ActionType.SCALE_CAPACITY,
            "throughput_saturation": ActionType.SCALE_CAPACITY,
            "capacity_limit": ActionType.SCALE_CAPACITY,
            
            # Critical issues requiring escalation
            "fiber_cut": ActionType.ESCALATE_ISSUE,
            "multiple_node_failure": ActionType.ESCALATE_ISSUE,
            "unknown_root_cause": ActionType.ESCALATE_ISSUE
        }
        
        logger.info(f"Incident Manager initialized with model: {model_endpoint}")
    
    def _call_foundation_model(self, prompt: str, max_tokens: int = 1500) -> Dict[str, Any]:
        """Call Databricks Foundation Model for incident analysis"""
        
        try:
            # Debug logging for API configuration
            logger.info(f"🔧 Debug - DATABRICKS_HOST: {DATABRICKS_HOST}")
            logger.info(f"🔧 Debug - Model Endpoint: {self.model_endpoint}")
            logger.info(f"🔧 Debug - Token Length: {len(DATABRICKS_TOKEN) if DATABRICKS_TOKEN else 0}")
            
            url = f"https://{DATABRICKS_HOST}/serving-endpoints/{self.model_endpoint}/invocations"
            logger.info(f"🔧 Debug - Full URL: {url}")
            
            headers = {
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert Network Incident Manager for telecom operations. Make precise operational decisions based on log analysis."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": max_tokens,
                "temperature": 0.2,  # Lower temperature for more consistent decisions
                "top_p": 0.9
            }
            
            response = requests.post(url, headers=headers, json=payload, timeout=180)
            
            if response.status_code == 200:
                result = response.json()
                
                # Extract response content (matching working format from notebooks 02, 03, 04)
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0]["message"]["content"]
                    return {
                        "success": True,
                        "content": content,
                        "usage": result.get("usage", {}),
                        "model": self.model_endpoint
                    }
                else:
                    return {"success": False, "error": "Invalid response format", "content": ""}
            else:
                logger.error(f"Foundation Model API error: {response.status_code}")
                logger.error(f"Response headers: {response.headers}")
                logger.error(f"Response text: {response.text}")
                return {"success": False, "error": f"API error: {response.status_code}", "content": ""}
                
        except Exception as e:
            logger.error(f"Foundation Model call failed: {e}")
            return {"success": False, "error": str(e), "content": ""}
    
    def _extract_components_from_logs(self, log_content: str) -> List[str]:
        """Extract affected network components from log content"""
        
        components = []
        log_lines = log_content.split('\n')
        
        # Patterns to identify network components
        component_patterns = [
            r'Node-([A-Za-z0-9-]+)',
            r'Router-([A-Za-z0-9-]+)',
            r'Switch-([A-Za-z0-9-]+)',
            r'Cell-([A-Za-z0-9-]+)',
            r'eNB-([A-Za-z0-9-]+)',
            r'gNB-([A-Za-z0-9-]+)',
            r'Core-([A-Za-z0-9-]+)',
            r'DNS-([A-Za-z0-9-]+)',
            r'BGP-([A-Za-z0-9-]+)'
        ]
        
        for line in log_lines:
            for pattern in component_patterns:
                matches = re.findall(pattern, line)
                for match in matches:
                    component_name = f"{pattern.split('(')[0].replace('-', '')}-{match}"
                    if component_name not in components:
                        components.append(component_name)
        
        return components[:10]  # Limit to 10 components for manageable analysis
    
    def _estimate_user_impact(self, log_content: str, severity: str) -> int:
        """Estimate number of affected users based on logs and severity"""
        
        # Extract user counts from logs if explicitly mentioned
        user_patterns = [
            r'(\d+,?\d*)\s+(?:users?|customers?)\s+(?:affected|impacted)',
            r'affecting\s+(\d+,?\d*)\s+(?:users?|customers?)',
            r'impact.*?(\d+,?\d*)\s+(?:users?|customers?)'
        ]
        
        for pattern in user_patterns:
            matches = re.findall(pattern, log_content.lower())
            if matches:
                try:
                    # Take the highest number found
                    user_count = max([int(match.replace(',', '')) for match in matches])
                    return user_count
                except ValueError:
                    continue
        
        # Fallback estimates based on severity and component type
        severity_multipliers = {"P1": 50000, "P2": 15000, "P3": 5000}
        base_impact = severity_multipliers.get(severity, 5000)
        
        # Adjust based on component type
        if "core" in log_content.lower():
            return base_impact
        elif "node" in log_content.lower():
            return int(base_impact * 0.6)
        elif "cell" in log_content.lower():
            return int(base_impact * 0.3)
        else:
            return int(base_impact * 0.5)
    
    def _analyze_incident_patterns(self, log_content: str) -> Tuple[ActionType, Dict[str, str], str]:
        """Rule-based pattern analysis for incident decision making"""
        
        log_content_lower = log_content.lower()
        action_parameters = {}
        reasoning = []
        
        # Check for specific incident patterns
        if any(keyword in log_content_lower for keyword in ["node down", "heartbeat missed", "process crash"]):
            # Extract node ID
            node_matches = re.findall(r'node[^\w]*([a-zA-Z0-9-]+)', log_content_lower)
            if node_matches:
                action_parameters["node_id"] = node_matches[0]
                reasoning.append(f"Node {node_matches[0]} failure detected")
            return ActionType.RESTART_NODE, action_parameters, "; ".join(reasoning)
        
        elif any(keyword in log_content_lower for keyword in ["congestion", "overload", "prb.*9[0-9]%"]):
            # Extract cell and neighbor information
            cell_matches = re.findall(r'cell[^\w]*([a-zA-Z0-9-]+)', log_content_lower)
            if cell_matches:
                action_parameters["cell_id"] = cell_matches[0]
                action_parameters["neighbor_id"] = f"neighbor-{cell_matches[0]}-001"
                reasoning.append(f"Cell {cell_matches[0]} congestion detected")
            return ActionType.REROUTE_TRAFFIC, action_parameters, "; ".join(reasoning)
        
        elif any(keyword in log_content_lower for keyword in ["packet loss", "jitter", "voice quality", "mos"]):
            # QoS adjustment needed
            if "voice" in log_content_lower:
                action_parameters["profile"] = "voice_priority"
            else:
                action_parameters["profile"] = "data_optimization"
            reasoning.append("QoS degradation detected, profile adjustment needed")
            return ActionType.ADJUST_QOS, action_parameters, "; ".join(reasoning)
        
        elif any(keyword in log_content_lower for keyword in ["utilization.*8[5-9]%", "utilization.*9[0-9]%", "saturation"]):
            # Capacity scaling needed
            cell_matches = re.findall(r'cell[^\w]*([a-zA-Z0-9-]+)', log_content_lower)
            if cell_matches:
                action_parameters["cell_id"] = cell_matches[0]
                action_parameters["percent"] = "25"  # Default 25% capacity increase
                reasoning.append(f"Capacity scaling required for {cell_matches[0]}")
            return ActionType.SCALE_CAPACITY, action_parameters, "; ".join(reasoning)
        
        elif any(keyword in log_content_lower for keyword in ["fiber cut", "multiple.*fail", "persistent.*fail"]):
            reasoning.append("Critical infrastructure failure requiring escalation")
            return ActionType.ESCALATE_ISSUE, action_parameters, "; ".join(reasoning)
        
        elif any(keyword in log_content_lower for keyword in ["stabilized", "recovered", "normalized", "resolved"]):
            reasoning.append("Incident appears resolved, no action needed")
            return ActionType.NO_ACTION, action_parameters, "; ".join(reasoning)
        
        else:
            reasoning.append("Incident pattern requires escalation for manual review")
            return ActionType.ESCALATE_ISSUE, action_parameters, "; ".join(reasoning)
    
    def make_incident_decision(self, incident_input: IncidentInput) -> IncidentDecision:
        """
        Analyze incident and make operational decision
        """
        
        try:
            # Create decision analysis prompt
            decision_prompt = f"""
            Analyze this network incident and recommend the appropriate operational action:
            
            **INCIDENT DETAILS:**
            - Incident ID: {incident_input.incident_id}
            - Severity: {incident_input.severity_classification}
            - Detection Time: {incident_input.detected_timestamp}
            - Log File: {incident_input.log_file_path}
            
            **LOG CONTENT:**
            {incident_input.log_content}
            
            **AVAILABLE ACTIONS:**
            1. restart_node - For node failures, heartbeat issues, process crashes
            2. reroute_traffic - For congestion, cell overload, traffic management
            3. adjust_qos - For packet loss, jitter, voice quality issues
            4. scale_capacity - For high utilization (>85%), throughput saturation
            5. no_action_needed - For resolved/stabilized incidents
            6. escalate_issue - For fiber cuts, multiple failures, unknown causes
            
            **DECISION CRITERIA:**
            - Node down/heartbeat missed → restart_node
            - Severe congestion (>90% PRB) → reroute_traffic  
            - Packet loss/jitter → adjust_qos
            - Sustained >85% utilization → scale_capacity
            - Already stabilized/recovered → no_action_needed
            - Fiber cut/persistent issues → escalate_issue
            
            **RESPONSE FORMAT:**
            Respond with JSON containing:
            {{
                "recommended_action": "action_name",
                "action_parameters": {{"param1": "value1", "param2": "value2"}},
                "reasoning": "Brief explanation of decision",
                "confidence": 0.85,
                "priority": "HIGH|STANDARD|LOW",
                "escalation_needed": true/false,
                "estimated_users_affected": number
            }}
            """
            
            # Call Foundation Model for intelligent decision
            logger.info("🤖 Attempting Foundation Model decision making...")
            model_response = self._call_foundation_model(decision_prompt)
            
            if model_response["success"]:
                logger.info("✅ Foundation Model response received successfully")
                try:
                    # Parse JSON response
                    decision_data = json.loads(model_response["content"])
                except json.JSONDecodeError:
                    # Fallback to rule-based analysis
                    logger.warning("JSON parsing failed, using rule-based analysis")
                    action, params, reasoning = self._analyze_incident_patterns(incident_input.log_content)
                    decision_data = {
                        "recommended_action": action.value,
                        "action_parameters": params,
                        "reasoning": reasoning,
                        "confidence": 0.75,
                        "priority": "HIGH" if incident_input.severity_classification == "P1" else "STANDARD",
                        "escalation_needed": action == ActionType.ESCALATE_ISSUE,
                        "estimated_users_affected": self._estimate_user_impact(
                            incident_input.log_content, incident_input.severity_classification
                        )
                    }
            else:
                # Fallback to rule-based analysis
                logger.warning("Foundation Model unavailable, using rule-based analysis")
                logger.warning(f"API Error Details: {model_response.get('error', 'Unknown error')}")
                action, params, reasoning = self._analyze_incident_patterns(incident_input.log_content)
                decision_data = {
                    "recommended_action": action.value,
                    "action_parameters": params,
                    "reasoning": reasoning,
                    "confidence": 0.70,
                    "priority": "HIGH" if incident_input.severity_classification == "P1" else "STANDARD",
                    "escalation_needed": action == ActionType.ESCALATE_ISSUE,
                    "estimated_users_affected": self._estimate_user_impact(
                        incident_input.log_content, incident_input.severity_classification
                    )
                }
            
            # Extract affected components
            affected_components = self._extract_components_from_logs(incident_input.log_content)
            
            # Create decision output
            decision = IncidentDecision(
                decision_id=f"DEC_{incident_input.incident_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                incident_id=incident_input.incident_id,
                recommended_action=ActionType(decision_data["recommended_action"]),
                action_parameters=decision_data.get("action_parameters", {}),
                confidence_score=decision_data.get("confidence", 0.75),
                reasoning=decision_data.get("reasoning", "Decision based on incident analysis"),
                affected_components=affected_components,
                estimated_impact_users=decision_data.get("estimated_users_affected", 0),
                priority_level=PriorityLevel(decision_data.get("priority", "STANDARD")),
                escalation_required=decision_data.get("escalation_needed", False)
            )
            
            logger.info(f"✅ Decision made for incident {incident_input.incident_id}")
            logger.info(f"   • Action: {decision.recommended_action.value}")
            logger.info(f"   • Confidence: {decision.confidence_score:.2f}")
            logger.info(f"   • Priority: {decision.priority_level.value}")
            
            return decision
            
        except Exception as e:
            logger.error(f"Decision making failed for incident {incident_input.incident_id}: {e}")
            raise
    
    def save_decision_to_unity_catalog(self, decision: IncidentDecision, log_file_path: str, severity: str) -> bool:
        """Save incident decision to Unity Catalog"""
        
        try:
            # Prepare decision data
            decision_data = [
                (
                    decision.incident_id,
                    log_file_path,
                    severity,
                    decision.recommended_action.value,
                    decision.action_parameters,
                    decision.confidence_score,
                    decision.reasoning,
                    decision.affected_components,
                    int(decision.estimated_impact_users),
                    decision.priority_level.value,
                    decision.escalation_required,
                    self.agent_version
                )
            ]
            
            # Column names
            columns = [
                "incident_id", "log_file_path", "severity_classification",
                "recommended_action", "action_parameters", "confidence_score",
                "reasoning", "affected_components", "estimated_impact_users",
                "priority_level", "escalation_required", "agent_version"
            ]
            
            # Create DataFrame with explicit schema to ensure proper data types
            from pyspark.sql.types import StructType, StructField, StringType, MapType, DoubleType, ArrayType, BooleanType, LongType
            
            schema = StructType([
                StructField("incident_id", StringType(), False),
                StructField("log_file_path", StringType(), True),
                StructField("severity_classification", StringType(), False),
                StructField("recommended_action", StringType(), False),
                StructField("action_parameters", MapType(StringType(), StringType()), True),
                StructField("confidence_score", DoubleType(), True),
                StructField("reasoning", StringType(), True),
                StructField("affected_components", ArrayType(StringType()), True),
                StructField("estimated_impact_users", LongType(), True),
                StructField("priority_level", StringType(), True),
                StructField("escalation_required", BooleanType(), True),
                StructField("agent_version", StringType(), True)
            ])
            
            # Create DataFrame and save with schema conflict handling
            df = spark.createDataFrame(decision_data, schema)
            
            try:
                # Try append mode first
                df.write \
                  .format("delta") \
                  .mode("append") \
                  .saveAsTable(UC_TABLE_INCIDENTS)
            except Exception as append_error:
                # If schema conflict, recreate table
                logger.warning(f"Schema conflict detected, recreating table: {append_error}")
                spark.sql(f"DROP TABLE IF EXISTS {UC_TABLE_INCIDENTS}")
                setup_incident_manager_tables()
                # Try append again
                df.write \
                  .format("delta") \
                  .mode("append") \
                  .saveAsTable(UC_TABLE_INCIDENTS)
            
            logger.info(f"✅ Decision saved to Unity Catalog: {UC_TABLE_INCIDENTS}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save decision to Unity Catalog: {e}")
            return False

# Initialize Incident Manager
incident_manager = DatabricksIncidentManager()
print("✅ Incident Manager Agent initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Testing & Validation

# COMMAND ----------

def test_incident_manager():
    """Test Incident Manager with various incident scenarios"""
    
    print("🧪 Testing Incident Manager Agent...")
    print("=" * 60)
    
    # Test scenarios
    test_incidents = [
        {
            "name": "Critical Node Failure",
            "incident": IncidentInput(
                incident_id="INC-20250907-001",
                log_file_path="/logs/critical_node_failure.txt",
                log_content="""
                [2025-09-07 10:15:30] CRITICAL Node-5G-Core-001: Heartbeat missed for 60 seconds
                [2025-09-07 10:15:35] ERROR Node-5G-Core-001: Process crash detected - radio_mgmt_service
                [2025-09-07 10:15:40] ALERT Network-Ops: 25,000 users affected by node failure
                [2025-09-07 10:16:00] ERROR BGP-Router-001: Neighbor 10.0.1.1 is unreachable
                """,
                severity_classification="P1",
                detected_timestamp=datetime(2025, 9, 7, 10, 15, 30)
            ),
            "expected_action": ActionType.RESTART_NODE
        },
        {
            "name": "Traffic Congestion",
            "incident": IncidentInput(
                incident_id="INC-20250907-002", 
                log_file_path="/logs/traffic_congestion.txt",
                log_content="""
                [2025-09-07 14:22:10] WARN Cell-LTE-023: PRB utilization at 94%
                [2025-09-07 14:22:15] ERROR Cell-LTE-023: Severe congestion detected
                [2025-09-07 14:22:20] INFO Network-Ops: 3,500 users experiencing degraded service
                [2025-09-07 14:22:25] WARN Neighbor-Cell-024: Available capacity 40%
                """,
                severity_classification="P2",
                detected_timestamp=datetime(2025, 9, 7, 14, 22, 10)
            ),
            "expected_action": ActionType.REROUTE_TRAFFIC
        },
        {
            "name": "QoS Degradation", 
            "incident": IncidentInput(
                incident_id="INC-20250907-003",
                log_file_path="/logs/qos_degradation.txt",
                log_content="""
                [2025-09-07 16:45:12] WARN Voice-Service: MOS score dropped to 3.2
                [2025-09-07 16:45:15] ERROR QoS-Engine: Packet loss increased to 2.3%
                [2025-09-07 16:45:18] ALERT Voice-Service: Jitter exceeding 50ms threshold
                [2025-09-07 16:45:20] INFO Network-Ops: 1,200 voice calls affected
                """,
                severity_classification="P2",
                detected_timestamp=datetime(2025, 9, 7, 16, 45, 12)
            ),
            "expected_action": ActionType.ADJUST_QOS
        },
        {
            "name": "Resolved Incident",
            "incident": IncidentInput(
                incident_id="INC-20250907-004",
                log_file_path="/logs/resolved_incident.txt", 
                log_content="""
                [2025-09-07 18:10:05] INFO Node-WiFi-102: Maintenance completed successfully
                [2025-09-07 18:10:10] INFO All-Systems: KPIs normalized across all interfaces
                [2025-09-07 18:10:15] INFO Network-Ops: Service quality stabilized
                [2025-09-07 18:10:20] INFO Monitoring: All alerts cleared
                """,
                severity_classification="P3",
                detected_timestamp=datetime(2025, 9, 7, 18, 10, 5)
            ),
            "expected_action": ActionType.NO_ACTION
        }
    ]
    
    results = []
    
    for i, test_case in enumerate(test_incidents, 1):
        print(f"\n{i}/4. Testing: {test_case['name']}")
        print("-" * 40)
        
        try:
            # Make decision
            decision = incident_manager.make_incident_decision(test_case["incident"])
            
            # Check if decision matches expected
            action_correct = decision.recommended_action == test_case["expected_action"]
            
            print(f"   • Expected Action: {test_case['expected_action'].value}")
            print(f"   • Actual Action: {decision.recommended_action.value}")
            print(f"   • Confidence: {decision.confidence_score:.2f}")
            print(f"   • Priority: {decision.priority_level.value}")
            print(f"   • Users Affected: {decision.estimated_impact_users:,}")
            print(f"   • Escalation Needed: {decision.escalation_required}")
            print(f"   • Reasoning: {decision.reasoning[:80]}...")
            print(f"   • Accuracy: {'✅ CORRECT' if action_correct else '❌ INCORRECT'}")
            
            # Save to Unity Catalog
            save_success = incident_manager.save_decision_to_unity_catalog(
                decision, 
                test_case["incident"].log_file_path,
                test_case["incident"].severity_classification
            )
            print(f"   • Unity Catalog Save: {'✅ Success' if save_success else '❌ Failed'}")
            
            results.append({
                "test_name": test_case["name"],
                "correct": action_correct,
                "confidence": decision.confidence_score,
                "decision": decision
            })
            
        except Exception as e:
            logger.error(f"Test failed for {test_case['name']}: {e}")
            print(f"   • Test Result: ❌ FAILED - {e}")
            results.append({"test_name": test_case["name"], "correct": False, "confidence": 0.0})
    
    # Test summary
    print(f"\n{'='*60}")
    print("🎯 INCIDENT MANAGER TEST SUMMARY")
    print(f"{'='*60}")
    
    correct_count = sum(1 for r in results if r["correct"])
    total_tests = len(results)
    accuracy = (correct_count / total_tests) * 100 if total_tests > 0 else 0
    avg_confidence = sum(r.get("confidence", 0) for r in results) / total_tests if total_tests > 0 else 0
    
    print(f"   • Total Tests: {total_tests}")
    print(f"   • Correct Decisions: {correct_count}")
    print(f"   • Decision Accuracy: {accuracy:.1f}%")
    print(f"   • Average Confidence: {avg_confidence:.2f}")
    print(f"   • Test Status: {'✅ PASSED' if accuracy >= 75 else '❌ FAILED'}")
    
    return accuracy >= 75

# Run the test
test_success = test_incident_manager()
print(f"\n🏆 Final Result: {'✅ INCIDENT MANAGER TESTS PASSED' if test_success else '❌ TESTS FAILED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Unity Catalog Data Verification

# COMMAND ----------

def display_unity_catalog_data():
    """Display data from Unity Catalog tables for verification"""
    
    try:
        print("🔍 UNITY CATALOG DATA VERIFICATION")
        print("=" * 70)
        
        # Check incident_decisions table
        print(f"\n📋 INCIDENT DECISIONS ({UC_TABLE_INCIDENTS}):")
        print("-" * 50)
        
        decisions_df = spark.sql(f"""
            SELECT 
                decision_id,
                incident_id,
                severity_classification,
                recommended_action,
                confidence_score,
                priority_level,
                estimated_impact_users,
                escalation_required,
                decision_timestamp
            FROM {UC_TABLE_INCIDENTS}
            ORDER BY decision_timestamp DESC
            LIMIT 10
        """)
        
        decisions_count = decisions_df.count()
        print(f"Total Records: {decisions_count}")
        
        if decisions_count > 0:
            decisions_df.show(truncate=False)
        else:
            print("No incident decisions found.")
        
        # Check network_actions table
        print(f"\n🔧 NETWORK ACTIONS ({UC_TABLE_ACTIONS}):")
        print("-" * 50)
        
        actions_df = spark.sql(f"""
            SELECT 
                action_id,
                incident_id,
                action_type,
                action_status,
                execution_timestamp,
                completion_timestamp,
                action_result,
                executed_by
            FROM {UC_TABLE_ACTIONS}
            ORDER BY action_id DESC
            LIMIT 10
        """)
        
        actions_count = actions_df.count()
        print(f"Total Records: {actions_count}")
        
        if actions_count > 0:
            actions_df.show(truncate=False)
        else:
            print("No network actions found.")
        
        # Show relationship between tables
        if decisions_count > 0 and actions_count > 0:
            print(f"\n🔗 INCIDENT → ACTION MAPPING:")
            print("-" * 50)
            
            joined_df = spark.sql(f"""
                SELECT 
                    d.incident_id,
                    d.recommended_action,
                    d.priority_level,
                    d.estimated_impact_users,
                    a.action_type,
                    a.action_status,
                    a.executed_by
                FROM {UC_TABLE_INCIDENTS} d
                LEFT JOIN {UC_TABLE_ACTIONS} a ON d.incident_id = a.incident_id
                ORDER BY d.decision_timestamp DESC
            """)
            
            joined_df.show(truncate=False)
        
        print(f"\n✅ Unity Catalog verification completed!")
        print(f"   • Incident Decisions: {decisions_count} records")
        print(f"   • Network Actions: {actions_count} records")
        
        return decisions_count, actions_count
        
    except Exception as e:
        print(f"❌ Error accessing Unity Catalog data: {e}")
        return 0, 0

# Display the verification data
incident_count, action_count = display_unity_catalog_data()

# COMMAND ----------

print("✅ Incident Manager Agent notebook execution completed successfully!")
print(f"📊 Summary: {incident_count} decisions recorded, {action_count} actions logged")
