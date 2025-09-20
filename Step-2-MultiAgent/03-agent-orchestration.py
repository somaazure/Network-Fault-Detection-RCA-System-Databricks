# Databricks notebook source
# MAGIC %md
# MAGIC # AI Agent Orchestration for Network RCA
# MAGIC 
# MAGIC This notebook orchestrates Databricks AI agents for severity classification and RCA generation.
# MAGIC It replaces the Azure Semantic Kernel implementation with Databricks Agent Bricks.

# COMMAND ----------

# MAGIC %pip install databricks-sdk requests

# COMMAND ----------

import os
import json
import asyncio
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
import requests

# Initialize Spark and Databricks clients
spark = SparkSession.builder \
    .appName("NetworkAgentOrchestration") \
    .getOrCreate()

w = WorkspaceClient()

# Set up Unity Catalog
spark.sql("USE CATALOG network_fault_detection")

print("✅ Agent orchestration environment initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Configuration

# COMMAND ----------

class DatabricksAgentConfig:
    """Configuration for Databricks AI Agents"""
    
    # Model serving endpoints
    SEVERITY_MODEL = "databricks-meta-llama-3-1-405b-instruct"  
    RCA_MODEL = "databricks-meta-llama-3-1-405b-instruct"
    
    # Agent prompts
    SEVERITY_PROMPT = """You are a Severity Classifier for Telecom incidents.
Analyze the following log content and classify the incident severity into:

- P1 (Critical): Complete outage, multiple nodes down, high revenue impact, emergency
  Examples: Core network failure, multiple eNB/gNB down, fiber cut affecting major area
- P2 (Major): Service degradation, affects many customers, significant performance impact
  Examples: Single node failure, congestion affecting >50% capacity, voice quality issues
- P3 (Minor): Isolated issue, early warning, minimal customer impact
  Examples: Single cell congestion, minor KPI degradation, preventive scaling

ANALYSIS CRITERIA:
- Customer impact scope (single cell vs multiple nodes vs region)
- Service availability (full outage vs degradation vs minor issues)
- KPI severity (>90% degradation = P1, 50-90% = P2, <50% = P3)
- Duration and persistence of issues

LOG CONTENT TO ANALYZE:
{log_content}

FORMAT:
- Analyze the log content above
- Respond ONLY with the severity code: P1, P2, or P3
- ALWAYS prepend: "SEVERITY_CLASSIFIER > "

RULES:
- Do NOT execute any corrective actions
- Focus only on severity classification
- Use the log content provided above to analyze the incident"""

    RCA_PROMPT = """You are the Root Cause Analysis (RCA) Agent.
Analyze the following log content and severity classification to produce an RCA report:

LOG CONTENT:
{log_content}

SEVERITY CLASSIFICATION:
{severity_classification}

TASKS:
1) Analyze the log content above
2) Use the severity classification provided
3) Produce a Markdown RCA with the following sections and headings exactly:
## Incident Summary
## Incident Severity
## Impact Analysis
## Root Cause
## Corrective Actions Taken
## Preventive Measures
## Incident Timestamp

4) In the "Incident Severity" section, include the P1/P2/P3 classification and justify it based on the log analysis.
5) Generate comprehensive, factual RCA content
6) Reply with the complete markdown RCA report

RULES:
- ALWAYS prepend: "RCA_AGENT > "
- Keep the RCA crisp and factual; no speculation when evidence is weak.
- Include severity classification and reasoning in the report.
- Use the log content provided above to analyze the incident"""

config = DatabricksAgentConfig()
print("✅ Agent configuration loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Agent Classes

# COMMAND ----------

class SeverityClassifierAgent:
    """Databricks-powered severity classification agent"""
    
    def __init__(self, workspace_client):
        self.client = workspace_client
        self.endpoint_name = config.SEVERITY_MODEL
        
    async def classify_severity(self, log_content: str) -> dict:
        """Classify incident severity using Databricks Foundation Model"""
        
        start_time = datetime.now()
        
        try:
            prompt = config.SEVERITY_PROMPT.format(log_content=log_content)
            
            # Call Databricks Model Serving
            response = self.client.serving_endpoints.query(
                name=self.endpoint_name,
                messages=[
                    ChatMessage(role=ChatMessageRole.USER, content=prompt)
                ],
                max_tokens=100,
                temperature=0.1
            )
            
            result = response.choices[0].message.content
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Extract severity classification
            if ">" in result:
                severity = result.split(">")[-1].strip()
            else:
                severity = result.strip()
                
            # Validate severity level
            if severity not in ["P1", "P2", "P3"]:
                severity = "P2"  # Default fallback
                
            return {
                "severity": f"SEVERITY_CLASSIFIER > {severity}",
                "raw_response": result,
                "execution_time_ms": execution_time,
                "status": "success",
                "model_endpoint": self.endpoint_name
            }
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return {
                "severity": "SEVERITY_CLASSIFIER > P2",  # Default fallback
                "raw_response": f"Error: {str(e)}",
                "execution_time_ms": execution_time,
                "status": "failed",
                "model_endpoint": self.endpoint_name,
                "error": str(e)
            }

class RCAGenerationAgent:
    """Databricks-powered RCA generation agent"""
    
    def __init__(self, workspace_client):
        self.client = workspace_client
        self.endpoint_name = config.RCA_MODEL
        
    async def generate_rca(self, log_content: str, severity_classification: str) -> dict:
        """Generate RCA report using Databricks Foundation Model"""
        
        start_time = datetime.now()
        
        try:
            prompt = config.RCA_PROMPT.format(
                log_content=log_content,
                severity_classification=severity_classification
            )
            
            # Call Databricks Model Serving
            response = self.client.serving_endpoints.query(
                name=self.endpoint_name,
                messages=[
                    ChatMessage(role=ChatMessageRole.USER, content=prompt)
                ],
                max_tokens=2000,
                temperature=0.3
            )
            
            result = response.choices[0].message.content
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Clean up response
            cleaned_result = result.replace("RCA_AGENT >", "").strip()
            
            return {
                "rca_content": cleaned_result,
                "raw_response": result,
                "execution_time_ms": execution_time,
                "status": "success",
                "model_endpoint": self.endpoint_name
            }
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return {
                "rca_content": f"Error generating RCA: {str(e)}",
                "raw_response": f"Error: {str(e)}",
                "execution_time_ms": execution_time,
                "status": "failed", 
                "model_endpoint": self.endpoint_name,
                "error": str(e)
            }

# Initialize agents
severity_agent = SeverityClassifierAgent(w)
rca_agent = RCAGenerationAgent(w)

print("✅ AI agents initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Orchestration Pipeline

# COMMAND ----------

class NetworkIncidentOrchestrator:
    """Main orchestrator for processing network incidents with AI agents"""
    
    def __init__(self, spark_session, workspace_client):
        self.spark = spark_session
        self.client = workspace_client
        self.severity_agent = SeverityClassifierAgent(workspace_client)
        self.rca_agent = RCAGenerationAgent(workspace_client)
        
    def get_log_content_for_incident(self, incident_id: str) -> str:
        """Retrieve log content for a specific incident"""
        
        # Get incident details
        incident_df = self.spark.sql(f"""
            SELECT source_log_id, detected_timestamp, affected_nodes, incident_type
            FROM network_fault_detection.processed_data.incidents 
            WHERE incident_id = '{incident_id}'
        """)
        
        if incident_df.count() == 0:
            return f"No incident found with ID: {incident_id}"
        
        incident_row = incident_df.collect()[0]
        
        # Get related log entries from the timeframe
        log_content_df = self.spark.sql(f"""
            SELECT raw_content, timestamp, log_level, node_id, message
            FROM network_fault_detection.raw_data.network_logs
            WHERE timestamp BETWEEN '{incident_row.detected_timestamp}' - INTERVAL 30 MINUTES
              AND '{incident_row.detected_timestamp}' + INTERVAL 30 MINUTES
              AND node_id IN ({','.join([f"'{node}'" for node in incident_row.affected_nodes])})
            ORDER BY timestamp
        """)
        
        # Combine log entries into single content block
        log_entries = log_content_df.collect()
        
        if not log_entries:
            return f"No log content found for incident {incident_id}"
        
        combined_content = f"""
INCIDENT CONTEXT:
- Incident ID: {incident_id}
- Detected: {incident_row.detected_timestamp}
- Incident Type: {incident_row.incident_type}
- Affected Nodes: {', '.join(incident_row.affected_nodes)}

RELEVANT LOG ENTRIES:
""" + "\n".join([entry.raw_content for entry in log_entries])
        
        return combined_content
        
    async def process_single_incident(self, incident_id: str) -> dict:
        """Process a single incident through the AI agent pipeline"""
        
        print(f"🔄 Processing incident: {incident_id}")
        
        try:
            # Step 1: Get log content
            log_content = self.get_log_content_for_incident(incident_id)
            
            if log_content.startswith("No incident found") or log_content.startswith("No log content found"):
                return {
                    "incident_id": incident_id,
                    "status": "failed",
                    "error": log_content
                }
            
            # Step 2: Severity classification
            print(f"  🔍 Classifying severity...")
            severity_result = await self.severity_agent.classify_severity(log_content)
            
            # Log agent execution
            self.log_agent_execution(
                "severity_classifier",
                log_content[:500] + "...",
                severity_result["raw_response"],
                severity_result["execution_time_ms"],
                severity_result["status"],
                severity_result["model_endpoint"],
                severity_result.get("error")
            )
            
            # Step 3: RCA generation
            print(f"  📝 Generating RCA...")
            rca_result = await self.rca_agent.generate_rca(log_content, severity_result["severity"])
            
            # Log agent execution
            self.log_agent_execution(
                "rca_generator",
                f"Log content + severity: {severity_result['severity']}",
                rca_result["raw_response"][:500] + "...",
                rca_result["execution_time_ms"],
                rca_result["status"],
                rca_result["model_endpoint"],
                rca_result.get("error")
            )
            
            # Step 4: Save RCA report
            rca_id = self.save_rca_report(
                incident_id,
                severity_result["severity"],
                rca_result["rca_content"]
            )
            
            # Step 5: Send notifications
            self.send_incident_notifications(incident_id, severity_result["severity"], rca_id)
            
            print(f"  ✅ Incident {incident_id} processed successfully")
            
            return {
                "incident_id": incident_id,
                "status": "completed",
                "severity": severity_result["severity"],
                "rca_id": rca_id,
                "execution_time_ms": severity_result["execution_time_ms"] + rca_result["execution_time_ms"]
            }
            
        except Exception as e:
            print(f"  ❌ Error processing incident {incident_id}: {str(e)}")
            return {
                "incident_id": incident_id,
                "status": "failed", 
                "error": str(e)
            }
    
    def log_agent_execution(self, agent_name, input_data, output_data, execution_time_ms, status, model_endpoint, error_message=None):
        """Log agent execution to Unity Catalog"""
        
        execution_data = [
            (agent_name, input_data, output_data, execution_time_ms, model_endpoint, status, error_message, datetime.now())
        ]
        
        execution_df = self.spark.createDataFrame(
            execution_data,
            ["agent_name", "input_data", "output_data", "execution_time_ms", "model_endpoint", "execution_status", "error_message", "created_at"]
        )
        
        execution_df.write \
            .mode("append") \
            .saveAsTable("network_fault_detection.ml_models.agent_executions")
    
    def save_rca_report(self, incident_id: str, severity_classification: str, rca_content: str) -> str:
        """Save RCA report to Unity Catalog Delta table"""
        
        # Parse RCA sections (basic parsing)
        sections = self.parse_rca_sections(rca_content)
        
        rca_data = [(
            incident_id,
            "unknown",  # source_log_file - to be updated
            severity_classification,
            sections.get("root_cause", ""),
            sections.get("impact_analysis", ""),
            sections.get("corrective_actions", ""),
            sections.get("preventive_measures", ""),
            sections.get("incident_summary", ""),
            rca_content,
            "databricks_agent",
            datetime.now()
        )]
        
        rca_df = self.spark.createDataFrame(
            rca_data,
            ["incident_id", "source_log_file", "severity_classification", "root_cause", 
             "impact_analysis", "corrective_actions", "preventive_measures", 
             "incident_summary", "full_report", "generated_by", "created_at"]
        )
        
        rca_df.write \
            .mode("append") \
            .saveAsTable("network_fault_detection.processed_data.rca_reports")
        
        # Get the RCA ID (simplified - in real implementation would return actual ID)
        return f"rca_{incident_id}_{int(datetime.now().timestamp())}"
    
    def parse_rca_sections(self, rca_content: str) -> dict:
        """Basic parsing of RCA sections from markdown content"""
        
        sections = {}
        current_section = None
        current_content = []
        
        for line in rca_content.split('\n'):
            if line.strip().startswith('##'):
                # Save previous section
                if current_section:
                    sections[current_section] = '\n'.join(current_content).strip()
                
                # Start new section
                section_name = line.replace('##', '').strip().lower().replace(' ', '_')
                current_section = section_name
                current_content = []
            elif current_section:
                current_content.append(line)
        
        # Save final section
        if current_section:
            sections[current_section] = '\n'.join(current_content).strip()
        
        return sections
    
    def send_incident_notifications(self, incident_id: str, severity: str, rca_id: str):
        """Send Slack/Teams notifications for processed incidents"""
        
        # This would integrate with Unity Catalog notification functions
        notification_data = [{
            "incident_id": incident_id,
            "notification_type": "slack",
            "title": f"🚨 Incident {incident_id} Processed",
            "message": f"Severity: {severity}\nRCA ID: {rca_id}\nPlatform: Databricks Agent Bricks",
            "recipient": "network-ops-channel",
            "status": "sent",  # Simplified
            "sent_at": datetime.now()
        }]
        
        notification_df = self.spark.createDataFrame(
            notification_data,
            ["incident_id", "notification_type", "title", "message", "recipient", "status", "sent_at"]
        )
        
        notification_df.write \
            .mode("append") \
            .saveAsTable("network_fault_detection.operations.notifications")

# Initialize orchestrator
orchestrator = NetworkIncidentOrchestrator(spark, w)
print("✅ Network incident orchestrator initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Pending Incidents

# COMMAND ----------

async def process_pending_incidents():
    """Process all pending incidents from the agent triggers table"""
    
    # Get pending incidents
    pending_incidents_df = spark.sql("""
        SELECT incident_id, trigger_time
        FROM network_fault_detection.operations.agent_triggers
        WHERE agent_status = 'pending'
        ORDER BY trigger_time
        LIMIT 10
    """)
    
    pending_incidents = [row.incident_id for row in pending_incidents_df.collect()]
    
    if not pending_incidents:
        print("ℹ️ No pending incidents to process")
        return []
    
    print(f"🔄 Processing {len(pending_incidents)} pending incidents...")
    
    results = []
    
    for incident_id in pending_incidents:
        # Update status to processing
        spark.sql(f"""
            UPDATE network_fault_detection.operations.agent_triggers
            SET agent_status = 'processing'
            WHERE incident_id = '{incident_id}'
        """)
        
        # Process incident
        result = await orchestrator.process_single_incident(incident_id)
        results.append(result)
        
        # Update final status
        final_status = "completed" if result["status"] == "completed" else "failed"
        spark.sql(f"""
            UPDATE network_fault_detection.operations.agent_triggers
            SET agent_status = '{final_status}', processed_time = current_timestamp()
            WHERE incident_id = '{incident_id}'
        """)
    
    # Summary
    successful = sum(1 for r in results if r["status"] == "completed")
    failed = len(results) - successful
    
    print(f"📊 Processing Summary: {successful} completed, {failed} failed")
    
    return results

# Execute processing
# results = await process_pending_incidents()

print("ℹ️ Ready to process pending incidents (uncomment to execute)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Incident Processing

# COMMAND ----------

# Widget for manual incident processing
dbutils.widgets.text("manual_incident_id", "")
dbutils.widgets.dropdown("process_all_pending", "false", ["true", "false"])

manual_incident = dbutils.widgets.get("manual_incident_id")
process_all = dbutils.widgets.get("process_all_pending") == "true"

if manual_incident:
    print(f"🔄 Processing manual incident: {manual_incident}")
    # result = await orchestrator.process_single_incident(manual_incident)
    # print(f"Result: {result}")

if process_all:
    print("🔄 Processing all pending incidents...")
    # results = await process_pending_incidents()
    # print(f"Processed {len(results)} incidents")

print("ℹ️ Use widgets above to trigger manual processing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Metrics and Monitoring

# COMMAND ----------

def display_agent_metrics():
    """Display performance metrics for AI agents"""
    
    print("🤖 AI Agent Performance Metrics")
    print("=" * 50)
    
    # Agent execution summary
    agent_stats = spark.sql("""
        SELECT 
            agent_name,
            execution_status,
            COUNT(*) as executions,
            AVG(execution_time_ms) as avg_execution_time_ms,
            MAX(execution_time_ms) as max_execution_time_ms,
            MIN(execution_time_ms) as min_execution_time_ms
        FROM network_fault_detection.ml_models.agent_executions
        WHERE created_at >= current_timestamp() - INTERVAL 24 HOURS
        GROUP BY agent_name, execution_status
        ORDER BY agent_name, execution_status
    """)
    
    if agent_stats.count() > 0:
        print("Agent Execution Statistics (Last 24 Hours):")
        agent_stats.show(truncate=False)
    else:
        print("No agent executions in the last 24 hours")
    
    # Processing status summary
    processing_status = spark.sql("""
        SELECT 
            agent_status,
            COUNT(*) as count,
            MIN(trigger_time) as earliest_trigger,
            MAX(trigger_time) as latest_trigger
        FROM network_fault_detection.operations.agent_triggers
        WHERE trigger_time >= current_timestamp() - INTERVAL 24 HOURS
        GROUP BY agent_status
        ORDER BY count DESC
    """)
    
    if processing_status.count() > 0:
        print("\nIncident Processing Status (Last 24 Hours):")
        processing_status.show(truncate=False)
    else:
        print("No incident triggers in the last 24 hours")
    
    # RCA generation summary
    rca_summary = spark.sql("""
        SELECT 
            severity_classification,
            generated_by,
            COUNT(*) as reports_generated,
            AVG(LENGTH(full_report)) as avg_report_length
        FROM network_fault_detection.processed_data.rca_reports
        WHERE created_at >= current_timestamp() - INTERVAL 24 HOURS
        GROUP BY severity_classification, generated_by
        ORDER BY severity_classification, reports_generated DESC
    """)
    
    if rca_summary.count() > 0:
        print("\nRCA Report Generation (Last 24 Hours):")
        rca_summary.show(truncate=False)
    else:
        print("No RCA reports generated in the last 24 hours")

# Display metrics
display_agent_metrics()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Next Steps

# COMMAND ----------

print("✅ Agent orchestration notebook complete")
print("\nConfiguration Summary:")
print(f"- Severity Model: {config.SEVERITY_MODEL}")
print(f"- RCA Model: {config.RCA_MODEL}")
print(f"- Unity Catalog: network_fault_detection")

print("\nNext Steps:")
print("1. Enable streaming pipeline (notebook 02-streaming-pipeline.py)")
print("2. Use widgets above to process incidents manually")
print("3. Set up automated scheduling using Databricks Jobs")
print("4. Access dashboard (notebook 04-dashboard.sql)")
print("5. Configure Vector Search for RCA semantic search")

print("\n🔗 Integration Points:")
print("- Tables: incidents, rca_reports, agent_executions, notifications")
print("- Models: Databricks Foundation Models via Model Serving")
print("- Functions: Unity Catalog Functions for network operations")
print("- Monitoring: Built-in metrics and performance tracking")