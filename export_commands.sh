#!/bin/bash
# Databricks Workspace Export Commands for Network Fault Detection RCA System
# Based on your workspace structure: /Workspace/Users/somaazure@gmail.com/

# Set the base path for your workspace
WORKSPACE_BASE="/Workspace/Users/somaazure@gmail.com"
OUTPUT_DIR="./DBKformat_OP"

echo "🚀 Starting Databricks workspace export to SOURCE format..."
echo "📁 Output directory: $OUTPUT_DIR"

# Create directory structure
mkdir -p "$OUTPUT_DIR/AgentBricks"
mkdir -p "$OUTPUT_DIR/AgentBricks/Autoloader-Streaming"
mkdir -p "$OUTPUT_DIR/AgentBricks/Autoloader-Streaming/FIXED"
mkdir -p "$OUTPUT_DIR/AgentBricks/Autoloader-Streaming/FIXED/RAG"
mkdir -p "$OUTPUT_DIR/Step-2-MultiAgent"
mkdir -p "$OUTPUT_DIR/MCP-RAG"
mkdir -p "$OUTPUT_DIR/Flask_Apps"

echo "📋 Exporting Core Agent System..."

# Core Severity Classification Agent
databricks workspace export "$WORKSPACE_BASE/Severity_Agent_Databricks_Notebook" "$OUTPUT_DIR/01_Severity_Classification_Agent.py" --format SOURCE

# Core Serving Endpoints
databricks workspace export "$WORKSPACE_BASE/Create_Serving_Endpoint" "$OUTPUT_DIR/Create_Serving_Endpoint.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Create_Serving_Endpoint_BzUsecases-added" "$OUTPUT_DIR/Create_Serving_Endpoint_Enhanced.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Create_Serving_Endpoint_Org" "$OUTPUT_DIR/Create_Serving_Endpoint_Original.py" --format SOURCE

echo "📋 Exporting AgentBricks System..."

# AgentBricks Core Agents
databricks workspace export "$WORKSPACE_BASE/AgentBricks/01_Incident_Manager_AgentBricks" "$OUTPUT_DIR/AgentBricks/01_Incident_Manager_AgentBricks.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/AgentBricks/01_Incident_Manager_AgentBricks_NoUCDrop" "$OUTPUT_DIR/AgentBricks/01_Incident_Manager_AgentBricks_NoUCDrop.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/AgentBricks/02_Severity_Classification_AgentBricks" "$OUTPUT_DIR/AgentBricks/02_Severity_Classification_AgentBricks.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/AgentBricks/03_Network_Ops_Agent_AgentBricks" "$OUTPUT_DIR/AgentBricks/03_Network_Ops_Agent_AgentBricks.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/AgentBricks/04_RCA_Agent_AgentBricks_Initial" "$OUTPUT_DIR/AgentBricks/04_RCA_Agent_AgentBricks_Initial.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/AgentBricks/05_Multi_Agent_Orchestrator_AgentBricks_Initial" "$OUTPUT_DIR/AgentBricks/05_Multi_Agent_Orchestrator_AgentBricks_Initial.py" --format SOURCE

# AgentBricks Autoloader Streaming
databricks workspace export "$WORKSPACE_BASE/AgentBricks/Autoloader-Streaming/02_Severity_Classification_Agent_PRODUCTION_READY" "$OUTPUT_DIR/AgentBricks/Autoloader-Streaming/02_Severity_Classification_Agent_PRODUCTION_READY.py" --format SOURCE

# AgentBricks RAG Components
databricks workspace export "$WORKSPACE_BASE/AgentBricks/Autoloader-Streaming/FIXED/RAG/RAG_LangC/RAG_prod/02_Databricks_Vector_Search_Index_Creation" "$OUTPUT_DIR/AgentBricks/Autoloader-Streaming/FIXED/RAG_Vector_Search_Setup.py" --format SOURCE

echo "📋 Exporting Multi-Agent Pipeline..."

# Step-2-MultiAgent Components
databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/01_Incident_Manager_Agent_Databricks_Notebook" "$OUTPUT_DIR/Step-2-MultiAgent/01_Incident_Manager_Agent.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/01_Incident_Manager_Agent_Databricks_Notebook-FALLBACK" "$OUTPUT_DIR/Step-2-MultiAgent/01_Incident_Manager_Agent_FALLBACK.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/01_Incident_Manager_Agent_Databricks_Notebook_old" "$OUTPUT_DIR/Step-2-MultiAgent/01_Incident_Manager_Agent_old.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/02_Network_Ops_Agent_Databricks_Notebook_Fixed" "$OUTPUT_DIR/Step-2-MultiAgent/02_Network_Ops_Agent_Fixed.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/03_RCA_Agent_Databricks_Notebook_Fixed" "$OUTPUT_DIR/Step-2-MultiAgent/03_RCA_Agent_Fixed.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/03_RCA_Agent_Databricks_Notebook_Fixed-FALLBACK" "$OUTPUT_DIR/Step-2-MultiAgent/03_RCA_Agent_Fixed_FALLBACK.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/04_Multi_Agent_Orchestrator_Databricks_Notebook_Fixed" "$OUTPUT_DIR/Step-2-MultiAgent/04_Multi_Agent_Orchestrator_Fixed.py" --format SOURCE

echo "📋 Exporting MCP-RAG System..."

# MCP-RAG Components
databricks workspace export "$WORKSPACE_BASE/MCP-RAG/MCP_Test" "$OUTPUT_DIR/MCP-RAG/MCP_Test.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/MCP-RAG/MCP_RAG_Production_Simple_Fixed_Notebook" "$OUTPUT_DIR/MCP-RAG/MCP_RAG_Production_Fixed.py" --format SOURCE

echo "📋 Exporting Flask Applications..."

# Flask Applications
databricks workspace export "$WORKSPACE_BASE/Depoly_WS_Flask/deploy_workspace_flask_fixed" "$OUTPUT_DIR/Flask_Apps/deploy_workspace_flask_fixed.py" --format SOURCE

echo "📋 Exporting Test and Demo Notebooks..."

# Demo and Testing Notebooks
databricks workspace export "$WORKSPACE_BASE/Demo_UC-added" "$OUTPUT_DIR/Demo_UC_Enhanced.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Rag_test" "$OUTPUT_DIR/RAG_Test.py" --format SOURCE

# Development Notebooks (with timestamps - clean names)
databricks workspace export "$WORKSPACE_BASE/Untitled Notebook 2025-09-07 12:31:40" "$OUTPUT_DIR/Development_Notebook_2025_09_07.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Untitled Notebook 2025-09-10 21:22:32" "$OUTPUT_DIR/Development_Notebook_2025_09_10.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Untitled Notebook 2025-09-11 19:15:06" "$OUTPUT_DIR/Development_Notebook_2025_09_11.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Untitled Notebook 2025-09-14 14:03:36" "$OUTPUT_DIR/Development_Notebook_2025_09_14.py" --format SOURCE
databricks workspace export "$WORKSPACE_BASE/Untitled Notebook 2025-09-18 10:51:47" "$OUTPUT_DIR/Development_Notebook_2025_09_18.py" --format SOURCE

echo "✅ Export commands generated!"
echo "📝 To execute these exports, run:"
echo "   cd C:\\databricks-export\\GitUpload"
echo "   bash export_commands.sh"
echo ""
echo "⚠️  Make sure you have:"
echo "   1. Databricks CLI installed and configured"
echo "   2. Valid authentication token"
echo "   3. Access to the workspace: /Workspace/Users/somaazure@gmail.com/"