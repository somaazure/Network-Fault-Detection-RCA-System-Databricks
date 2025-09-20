# PowerShell Script for Databricks Workspace Export
# Network Fault Detection RCA System - Clean SOURCE format exports

# Set execution policy for current session
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process -Force

# Configuration
$WORKSPACE_BASE = "/Workspace/Users/somaazure@gmail.com"
$OUTPUT_DIR = "./DBKformat_OP"

Write-Host "🚀 Starting Databricks workspace export to SOURCE format..." -ForegroundColor Green
Write-Host "📁 Output directory: $OUTPUT_DIR" -ForegroundColor Yellow

# Create directory structure
New-Item -ItemType Directory -Path "$OUTPUT_DIR/AgentBricks" -Force | Out-Null
New-Item -ItemType Directory -Path "$OUTPUT_DIR/AgentBricks/Autoloader-Streaming" -Force | Out-Null
New-Item -ItemType Directory -Path "$OUTPUT_DIR/AgentBricks/Autoloader-Streaming/FIXED" -Force | Out-Null
New-Item -ItemType Directory -Path "$OUTPUT_DIR/Step-2-MultiAgent" -Force | Out-Null
New-Item -ItemType Directory -Path "$OUTPUT_DIR/MCP-RAG" -Force | Out-Null
New-Item -ItemType Directory -Path "$OUTPUT_DIR/Flask_Apps" -Force | Out-Null

Write-Host "📋 Exporting Core Agent System..." -ForegroundColor Cyan

# Core Severity Classification Agent
& databricks workspace export "$WORKSPACE_BASE/Severity_Agent_Databricks_Notebook" "$OUTPUT_DIR/01_Severity_Classification_Agent.py" --format SOURCE

# Core Serving Endpoints
& databricks workspace export "$WORKSPACE_BASE/Create_Serving_Endpoint" "$OUTPUT_DIR/Create_Serving_Endpoint.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Create_Serving_Endpoint_BzUsecases-added" "$OUTPUT_DIR/Create_Serving_Endpoint_Enhanced.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Create_Serving_Endpoint_Org" "$OUTPUT_DIR/Create_Serving_Endpoint_Original.py" --format SOURCE

Write-Host "📋 Exporting AgentBricks System..." -ForegroundColor Cyan

# AgentBricks Core Agents
& databricks workspace export "$WORKSPACE_BASE/AgentBricks/01_Incident_Manager_AgentBricks" "$OUTPUT_DIR/AgentBricks/01_Incident_Manager_AgentBricks.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/AgentBricks/01_Incident_Manager_AgentBricks_NoUCDrop" "$OUTPUT_DIR/AgentBricks/01_Incident_Manager_AgentBricks_NoUCDrop.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/AgentBricks/02_Severity_Classification_AgentBricks" "$OUTPUT_DIR/AgentBricks/02_Severity_Classification_AgentBricks.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/AgentBricks/03_Network_Ops_Agent_AgentBricks" "$OUTPUT_DIR/AgentBricks/03_Network_Ops_Agent_AgentBricks.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/AgentBricks/04_RCA_Agent_AgentBricks_Initial" "$OUTPUT_DIR/AgentBricks/04_RCA_Agent_AgentBricks_Initial.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/AgentBricks/05_Multi_Agent_Orchestrator_AgentBricks_Initial" "$OUTPUT_DIR/AgentBricks/05_Multi_Agent_Orchestrator_AgentBricks_Initial.py" --format SOURCE

# AgentBricks Autoloader Streaming
& databricks workspace export "$WORKSPACE_BASE/AgentBricks/Autoloader-Streaming/02_Severity_Classification_Agent_PRODUCTION_READY" "$OUTPUT_DIR/AgentBricks/Autoloader-Streaming/02_Severity_Classification_Agent_PRODUCTION_READY.py" --format SOURCE

Write-Host "📋 Exporting Multi-Agent Pipeline..." -ForegroundColor Cyan

# Step-2-MultiAgent Components
& databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/01_Incident_Manager_Agent_Databricks_Notebook" "$OUTPUT_DIR/Step-2-MultiAgent/01_Incident_Manager_Agent.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/01_Incident_Manager_Agent_Databricks_Notebook-FALLBACK" "$OUTPUT_DIR/Step-2-MultiAgent/01_Incident_Manager_Agent_FALLBACK.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/01_Incident_Manager_Agent_Databricks_Notebook_old" "$OUTPUT_DIR/Step-2-MultiAgent/01_Incident_Manager_Agent_old.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/02_Network_Ops_Agent_Databricks_Notebook_Fixed" "$OUTPUT_DIR/Step-2-MultiAgent/02_Network_Ops_Agent_Fixed.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/03_RCA_Agent_Databricks_Notebook_Fixed" "$OUTPUT_DIR/Step-2-MultiAgent/03_RCA_Agent_Fixed.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/03_RCA_Agent_Databricks_Notebook_Fixed-FALLBACK" "$OUTPUT_DIR/Step-2-MultiAgent/03_RCA_Agent_Fixed_FALLBACK.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Step-2-MultiAgent/04_Multi_Agent_Orchestrator_Databricks_Notebook_Fixed" "$OUTPUT_DIR/Step-2-MultiAgent/04_Multi_Agent_Orchestrator_Fixed.py" --format SOURCE

Write-Host "📋 Exporting MCP-RAG System..." -ForegroundColor Cyan

# MCP-RAG Components
& databricks workspace export "$WORKSPACE_BASE/MCP-RAG/MCP_Test" "$OUTPUT_DIR/MCP-RAG/MCP_Test.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/MCP-RAG/MCP_RAG_Production_Simple_Fixed_Notebook" "$OUTPUT_DIR/MCP-RAG/MCP_RAG_Production_Fixed.py" --format SOURCE

Write-Host "📋 Exporting Flask Applications..." -ForegroundColor Cyan

# Flask Applications
& databricks workspace export "$WORKSPACE_BASE/Depoly_WS_Flask/deploy_workspace_flask_fixed" "$OUTPUT_DIR/Flask_Apps/deploy_workspace_flask_fixed.py" --format SOURCE

Write-Host "📋 Exporting Test and Demo Notebooks..." -ForegroundColor Cyan

# Demo and Testing Notebooks
& databricks workspace export "$WORKSPACE_BASE/Demo_UC-added" "$OUTPUT_DIR/Demo_UC_Enhanced.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Rag_test" "$OUTPUT_DIR/RAG_Test.py" --format SOURCE

# Development Notebooks (with clean names)
& databricks workspace export "$WORKSPACE_BASE/Untitled Notebook 2025-09-07 12:31:40" "$OUTPUT_DIR/Development_Notebook_2025_09_07.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Untitled Notebook 2025-09-10 21:22:32" "$OUTPUT_DIR/Development_Notebook_2025_09_10.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Untitled Notebook 2025-09-11 19:15:06" "$OUTPUT_DIR/Development_Notebook_2025_09_11.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Untitled Notebook 2025-09-14 14:03:36" "$OUTPUT_DIR/Development_Notebook_2025_09_14.py" --format SOURCE
& databricks workspace export "$WORKSPACE_BASE/Untitled Notebook 2025-09-18 10:51:47" "$OUTPUT_DIR/Development_Notebook_2025_09_18.py" --format SOURCE

Write-Host "✅ Export commands completed!" -ForegroundColor Green
Write-Host "📂 All files exported to: $OUTPUT_DIR" -ForegroundColor Yellow
Write-Host "🔍 Check the files and then run git_setup.ps1 to push to GitHub" -ForegroundColor Cyan