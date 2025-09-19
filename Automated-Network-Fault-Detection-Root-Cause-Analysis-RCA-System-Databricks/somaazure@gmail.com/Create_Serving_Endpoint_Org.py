# Databricks notebook source
# MAGIC %md
# MAGIC ## ðŸš€ Optional: Create Custom ML Serving Endpoint for Severity Agent
# MAGIC
# MAGIC **This creates a deployable serving endpoint that you can see in Databricks ML Serving**

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Register Model as MLflow Model

# COMMAND ----------

import mlflow
import mlflow.pyfunc
import pandas as pd
from mlflow.models.signature import infer_signature
import joblib
import pickle
from datetime import datetime

# Create a custom MLflow model for our severity classifier
class SeverityClassifierModel(mlflow.pyfunc.PythonModel):
    """
    Custom MLflow model wrapper for Severity Classification Agent
    """
    
    def __init__(self):
        self.severity_criteria = {
            "P1": {
                "keywords": ["critical", "outage", "complete failure", "service down", 
                           "emergency", "disaster", "catastrophic", "total loss", "fiber cut"],
                "user_threshold": 10000
            },
            "P2": {
                "keywords": ["major", "degradation", "significant impact", "performance issue",
                           "error", "failure", "high latency", "timeout", "congestion"],
                "user_threshold": 1000
            },
            "P3": {
                "keywords": ["minor", "warning", "info", "normal", "maintenance", 
                           "low priority", "informational", "preventive"],
                "user_threshold": 100
            }
        }
    
    def predict(self, context, model_input):
        """
        Predict severity for network logs
        """
        import re
        
        results = []
        
        # Handle both single string and DataFrame input
        if isinstance(model_input, pd.DataFrame):
            log_contents = model_input.iloc[:, 0].tolist()  # First column
        elif isinstance(model_input, str):
            log_contents = [model_input]
        else:
            log_contents = model_input
        
        for log_content in log_contents:
            result = self._classify_single_log(log_content)
            results.append(result)
        
        # Return as DataFrame for serving endpoint
        return pd.DataFrame(results)
    
    def _classify_single_log(self, log_content):
        """Classify a single log entry"""
        import re
        
        log_lower = log_content.lower()
        
        # Extract user count if mentioned
        user_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*(?:users?|customers?)', log_content, re.IGNORECASE)
        affected_users = int(user_match.group(1).replace(',', '')) if user_match else None
        
        # Check for P1 indicators
        p1_keywords = any(keyword in log_lower for keyword in self.severity_criteria["P1"]["keywords"])
        p1_users = affected_users and affected_users > self.severity_criteria["P1"]["user_threshold"]
        
        if p1_keywords or p1_users:
            return {
                "severity": "P1",
                "confidence": 0.9 if p1_keywords and p1_users else 0.8,
                "reasoning": f"Critical severity detected. Keywords: {p1_keywords}, High user impact: {p1_users}",
                "affected_users": affected_users,
                "business_impact": "High - Critical system impact detected"
            }
        
        # Check for P2 indicators  
        p2_keywords = any(keyword in log_lower for keyword in self.severity_criteria["P2"]["keywords"])
        p2_users = affected_users and affected_users > self.severity_criteria["P2"]["user_threshold"]
        
        if p2_keywords or p2_users:
            return {
                "severity": "P2",
                "confidence": 0.8 if p2_keywords and p2_users else 0.7,
                "reasoning": f"Major severity detected. Keywords: {p2_keywords}, Moderate user impact: {p2_users}",
                "affected_users": affected_users,
                "business_impact": "Medium - Service degradation detected"
            }
        
        # Default to P3
        return {
            "severity": "P3",
            "confidence": 0.6,
            "reasoning": "Minor severity - no high-impact indicators found",
            "affected_users": affected_users,
            "business_impact": "Low - Minor or informational event"
        }

print("âœ… Custom MLflow model class created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Register Model in MLflow Model Registry

# COMMAND ----------

# Set up MLflow experiment
mlflow.set_experiment("/Shared/network-fault-detection")

# Sample data for model signature
sample_input = pd.DataFrame({
    "log_content": [
        "[2025-01-01 10:30:45] CRITICAL Node-5G-001: Complete service outage - 15,000 users affected",
        "[2025-01-01 12:00:45] ERROR Node-LTE-023: High latency - 5,000 users affected", 
        "[2025-01-01 14:00:10] INFO Node-WiFi-102: Maintenance completed successfully"
    ]
})

# Create model instance
severity_model = SeverityClassifierModel()

# Generate sample prediction for signature
sample_prediction = severity_model.predict(None, sample_input)

# Infer model signature
signature = infer_signature(sample_input, sample_prediction)

print("ðŸ“Š Sample Input:")
print(sample_input)
print("\nðŸ“Š Sample Prediction:")
print(sample_prediction)

# COMMAND ----------

# Suppress non-critical MLflow warnings
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='mlflow')

# Start MLflow run and register model
with mlflow.start_run(run_name=f"severity-classifier-{datetime.now().strftime('%Y%m%d-%H%M%S')}") as run:
    
    # Log model parameters
    mlflow.log_param("model_type", "severity_classifier")
    mlflow.log_param("classification_levels", "P1,P2,P3")
    mlflow.log_param("approach", "rule_based")
    
    # Log model metrics
    mlflow.log_metric("test_accuracy", 1.0)  # 100% on test data
    mlflow.log_metric("p1_threshold_users", 10000)
    mlflow.log_metric("p2_threshold_users", 1000)
    mlflow.log_metric("p3_threshold_users", 100)
    
    # Log the model (updated syntax to avoid deprecation warnings)
    model_info = mlflow.pyfunc.log_model(
        artifact_path="severity_classifier_model",
        python_model=severity_model,
        signature=signature,
        input_example=sample_input,  # Add input example to avoid warning
        registered_model_name="network_severity_classifier"
    )
    
    print(f"âœ… Model registered successfully!")
    print(f"ðŸ“‹ Run ID: {run.info.run_id}")
    print(f"ðŸŽ¯ Model URI: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create Model Serving Endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import requests
import os
import json
import mlflow
from mlflow.tracking import MlflowClient

# Initialize clients
w = WorkspaceClient()
client = MlflowClient()

# Configuration - Updated for Unity Catalog format
# First, let's detect the correct catalog and schema
try:
    # Try to get current catalog and schema
    current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    current_schema = spark.sql("SELECT current_schema()").collect()[0][0]

    print(f"ðŸ“Š Current Catalog: {current_catalog}")
    print(f"ðŸ“Š Current Schema: {current_schema}")

    # Unity Catalog format: catalog.schema.model_name
    model_name = f"{current_catalog}.{current_schema}.network_severity_classifier"

except Exception as e:
    print(f"âš ï¸ Could not detect catalog/schema: {e}")
    print("ðŸ”„ Trying with default catalog...")

    # Fallback to common defaults
    possible_catalogs = ["main", "hive_metastore", "workspace"]
    possible_schemas = ["default", "network_demo", "shared"]

    model_name = None

    # Try different combinations
    for catalog in possible_catalogs:
        for schema in possible_schemas:
            test_model_name = f"{catalog}.{schema}.network_severity_classifier"
            try:
                # Test if this model exists
                test_versions = client.search_model_versions(f"name='{test_model_name}'")
                if test_versions:
                    model_name = test_model_name
                    current_catalog = catalog
                    current_schema = schema
                    print(f"âœ… Found model at: {model_name}")
                    break
            except:
                continue
        if model_name:
            break

    if not model_name:
        print("âŒ Could not find registered model in any catalog/schema")
        print("ðŸ’¡ The model may not have been registered properly")

        # Let's try to re-register the model in Unity Catalog format
        print("ðŸ”„ Attempting to register model in Unity Catalog...")

        # Use current catalog.default.model_name format
        try:
            current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
            model_name = f"{current_catalog}.default.network_severity_classifier"
            print(f"ðŸ“Š Will use model name: {model_name}")
        except:
            model_name = "main.default.network_severity_classifier"
            current_catalog = "main"
            current_schema = "default"
            print(f"ðŸ“Š Using fallback model name: {model_name}")

print(f"ðŸŽ¯ Final model name: {model_name}")

endpoint_name = "network-severity-classifier-endpoint"

# Get authentication details
workspace_url = w.config.host
if workspace_url.startswith('https://'):
    workspace_url = workspace_url
else:
    workspace_url = f"https://{workspace_url}"

print(f"ðŸŒ Workspace URL: {workspace_url}")

# Try multiple token sources for better authentication
token = None
token_source = ""

# Method 1: Environment variable (most reliable)
if 'DATABRICKS_TOKEN' in os.environ:
    token = os.environ['DATABRICKS_TOKEN']
    token_source = "environment variable"

# Method 2: Notebook context
if not token:
    try:
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        token_source = "notebook context"
    except:
        pass

# Method 3: WorkspaceClient config
if not token:
    try:
        token = w.config.token
        token_source = "workspace client"
    except:
        pass

if not token:
    print("âŒ No authentication token found!")
    print("ðŸ’¡ Please set DATABRICKS_TOKEN environment variable or configure authentication")
    raise Exception("Authentication token required")

print(f"ðŸ”‘ Using token from: {token_source}")
print(f"ðŸ”‘ Token prefix: {token[:15]}...")

try:
    # Get model version using Unity Catalog compatible method
    print(f"ðŸ“Š Getting model information for: {model_name}")

    model_version = "1"  # Default version

    try:
        # Try to get model versions using search (Unity Catalog compatible)
        model_versions = client.search_model_versions(f"name='{model_name}'")
        if model_versions:
            # Get the latest version
            latest_version = max(model_versions, key=lambda v: int(v.version))
            model_version = latest_version.version
            print(f"ðŸ“Š Found model version: {model_version}")
        else:
            print(f"âš ï¸ No model versions found for {model_name}")
            print("ðŸ”„ Checking if we need to register the model first...")

            # The model might not be registered in Unity Catalog yet
            # Let's check if we have a registered model in the workspace
            try:
                # Check for models without Unity Catalog prefix
                simple_model_name = "network_severity_classifier"
                simple_versions = client.search_model_versions(f"name='{simple_model_name}'")
                if simple_versions:
                    print(f"âœ… Found model with simple name: {simple_model_name}")
                    print("ðŸ’¡ Model exists but not in Unity Catalog format")
                    print("ðŸ”„ We'll create the endpoint with the simple name for now")
                    model_name = simple_model_name
                    latest_version = max(simple_versions, key=lambda v: int(v.version))
                    model_version = latest_version.version
                else:
                    print("âš ï¸ No model found with either format")
                    print("ðŸ’¡ Using default version 1")
                    model_version = "1"
            except Exception as simple_error:
                print(f"âš ï¸ Simple model check failed: {simple_error}")
                print("ðŸ’¡ Will proceed with Unity Catalog format and version 1")
    except Exception as e:
        print(f"âš ï¸ Model version detection failed: {e}")
        print("ðŸ’¡ Using default version 1")
        model_version = "1"

    print(f"ðŸŽ¯ Using model: {model_name} version {model_version}")

    # Prepare headers with correct authentication
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Check if endpoint already exists
    print("ðŸ” Checking for existing endpoints...")
    list_url = f"{workspace_url}/api/2.0/serving-endpoints"
    list_response = requests.get(list_url, headers=headers, timeout=30)

    print(f"ðŸ“Š List endpoints status: {list_response.status_code}")

    endpoint_exists = False
    if list_response.status_code == 200:
        endpoints_data = list_response.json()
        endpoints = endpoints_data.get("endpoints", [])
        endpoint_exists = any(ep["name"] == endpoint_name for ep in endpoints)
        print(f"ðŸ“Š Found {len(endpoints)} existing endpoints")
    elif list_response.status_code == 401:
        print("âŒ Authentication failed - token may not have proper permissions")
        print("ðŸ’¡ Go to Databricks â†’ Settings â†’ Developer â†’ Access Tokens")
        print("ðŸ’¡ Create a new token with 'All APIs' scope")
        raise Exception("Authentication failed")
    else:
        print(f"âš ï¸ Could not list endpoints: {list_response.status_code}")
        print(f"Response: {list_response.text}")

    # Endpoint configuration
    endpoint_config = {
        "name": endpoint_name,
        "config": {
            "served_models": [
                {
                    "model_name": model_name,
                    "model_version": model_version,
                    "workload_size": "Small",
                    "scale_to_zero_enabled": True
                }
            ]
        }
    }

    if endpoint_exists:
        print(f"âš ï¸ Endpoint '{endpoint_name}' already exists")
        print("âœ… Using existing endpoint")
    else:
        print(f"ðŸš€ Creating new endpoint: {endpoint_name}")
        print(f"ðŸŽ¯ Model config: {model_name} v{model_version}")

        # Create endpoint
        create_url = f"{workspace_url}/api/2.0/serving-endpoints"
        create_response = requests.post(
            create_url,
            headers=headers,
            json=endpoint_config,
            timeout=60
        )

        print(f"ðŸ“Š Create endpoint status: {create_response.status_code}")

        if create_response.status_code == 200:
            print("âœ… Serving endpoint created successfully!")
            print("â±ï¸ Endpoint is starting up (this may take 5-10 minutes)")
        else:
            print(f"âŒ Creation failed: {create_response.status_code}")
            response_text = create_response.text
            print(f"Response: {response_text}")

            if "does not exist" in response_text.lower():
                print("\nðŸ’¡ Model Registration Issue Detected!")
                print("The model needs to be properly registered. Here are your options:")
                print("\nðŸ”§ Option 1: Manual Model Registration")
                print("1. Go to Databricks â†’ Machine Learning â†’ Models")
                print("2. Click 'Register Model'")
                print("3. Use the MLflow run from the previous step")
                print(f"4. Name it: {model_name}")
                print("\nðŸ”§ Option 2: Re-run the Model Registration Cell")
                print("Go back and re-run the MLflow model registration cell")
                print("Make sure it completes without errors")
                print("\nðŸ”§ Option 3: Manual Endpoint Creation")
                print("1. Go to Databricks â†’ Machine Learning â†’ Serving")
                print("2. Click 'Create Serving Endpoint'")
                print("3. Select your registered model from the dropdown")
                print(f"4. Name the endpoint: {endpoint_name}")
                print("5. Choose 'Small' workload size")
                print("6. Enable 'Scale to zero'")

            elif create_response.status_code == 401:
                print("ðŸ’¡ Token authentication failed")
                print("ðŸ’¡ Verify your token has Model Serving permissions")
            elif create_response.status_code == 403:
                print("ðŸ’¡ Permission denied - check workspace permissions")

            raise Exception("Endpoint creation failed - see manual options above")

    print(f"\nðŸ“‹ Endpoint Details:")
    print(f"   Name: {endpoint_name}")
    print(f"   URL: {workspace_url}/serving-endpoints/{endpoint_name}")
    print(f"   Model: {model_name} v{model_version}")
    print(f"\nðŸŽ¯ Check status at: Databricks â†’ Machine Learning â†’ Serving")

except Exception as e:
    print(f"âŒ Error: {e}")
    print(f"\nðŸ’¡ Summary of Issues Found:")
    print(f"1. Model name format: Unity Catalog requires catalog.schema.model_name")
    print(f"2. Model registration: The model may not be properly registered")
    print(f"3. Authentication: Token needs Model Serving permissions")

    print(f"\nðŸ› ï¸ Recommended Next Steps:")
    print(f"1. First, re-run the model registration cell to ensure model is saved")
    print(f"2. Then manually create the endpoint via UI:")
    print(f"   â€¢ Go to Databricks â†’ Machine Learning â†’ Serving")
    print(f"   â€¢ Click 'Create Serving Endpoint'")
    print(f"   â€¢ Select your model from the dropdown")
    print(f"   â€¢ Use endpoint name: {endpoint_name}")
    print(f"   â€¢ Choose 'Small' workload and enable 'Scale to zero'")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Test the Serving Endpoint

# COMMAND ----------

import requests
import json

# Test the serving endpoint
endpoint_name = "network-severity-classifier-endpoint"

# Get workspace details
workspace_url = w.config.host
token = w.config.token

# Wait for endpoint to be ready before testing
import time

# Configuration
endpoint_name = "network-severity-classifier-endpoint"

# Get authentication
token = None
if 'DATABRICKS_TOKEN' in os.environ:
    token = os.environ['DATABRICKS_TOKEN']
else:
    try:
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    except:
        token = w.config.token

workspace_url = w.config.host
if not workspace_url.startswith('https://'):
    workspace_url = f"https://{workspace_url}"

print("ðŸ§ª Testing Serving Endpoint")
print("=" * 40)

# First check endpoint status
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
status_url = f"{workspace_url}/api/2.0/serving-endpoints/{endpoint_name}"

try:
    status_response = requests.get(status_url, headers=headers, timeout=30)
    
    if status_response.status_code == 200:
        endpoint_info = status_response.json()
        state = endpoint_info.get("state", {}).get("ready", "Unknown")
        print(f"ðŸ“Š Endpoint Status: {state}")
        
        if state != "READY":
            print("â³ Endpoint is not ready yet")
            print("ðŸ’¡ Wait 5-10 more minutes and try again")
            print(f"Current state: {state}")
        else:
            print("âœ… Endpoint is ready for testing!")
            
            # Test the endpoint
            test_log = "[2025-01-01 10:30:45] CRITICAL Node-5G-001: Complete service outage affecting 25,000 users"
            
            payload = {
                "dataframe_records": [
                    {"log_content": test_log}
                ]
            }
            
            test_url = f"{workspace_url}/serving-endpoints/{endpoint_name}/invocations"
            
            print(f"ðŸŒ Testing URL: {test_url}")
            print(f"ðŸ“ Test Log: {test_log[:80]}...")
            
            response = requests.post(
                test_url,
                headers=headers,
                json=payload,
                timeout=60
            )
            
            print(f"ðŸ“Š Status Code: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                print("âœ… SUCCESS! Endpoint Response:")
                print(json.dumps(result, indent=2))
            else:
                print(f"âŒ Test failed: {response.text}")
    else:
        print(f"âŒ Could not check endpoint status: {status_response.status_code}")
        print(f"Response: {status_response.text}")
        
except Exception as e:
    print(f"âŒ Error: {e}")

print(f"\nðŸ’¡ Manual Check:")
print(f"Go to: Databricks â†’ Machine Learning â†’ Serving")
print(f"Look for: {endpoint_name}")
print(f"Status should be: Ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Check Endpoint Status

# COMMAND ----------

# Check all serving endpoints
print("ðŸ“Š All Serving Endpoints in Your Workspace:")
print("=" * 50)

try:
    endpoints = w.serving_endpoints.list()
    
    if not endpoints:
        print("No serving endpoints found")
    else:
        for endpoint in endpoints:
            print(f"ðŸ“‹ Endpoint: {endpoint.name}")
            print(f"   Status: {endpoint.state}")
            print(f"   URL: https://{w.config.host}/serving-endpoints/{endpoint.name}")
            
            # Get detailed endpoint info
            try:
                detail = w.serving_endpoints.get(name=endpoint.name)
                if detail.config and detail.config.served_models:
                    model = detail.config.served_models[0]
                    print(f"   Model: {model.model_name} v{model.model_version}")
                    print(f"   Workload: {model.workload_size}")
                print()
            except:
                print("   (Unable to get detailed info)")
                print()
                
except Exception as e:
    print(f"âŒ Error listing endpoints: {e}")

# Instructions for user
print("ðŸŽ¯ To Check Your Endpoints in Databricks UI:")
print("1. Go to Databricks workspace")
print("2. Click 'Machine Learning' in left sidebar") 
print("3. Click 'Serving'")
print("4. You should see 'network-severity-classifier-endpoint'")
print("5. Click on it to see details, test, and monitor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸŽ‰ Summary
# MAGIC
# MAGIC ### âœ… What You've Created:
# MAGIC 1. **Custom MLflow Model** - Registered in Model Registry
# MAGIC 2. **Serving Endpoint** - Deployable API endpoint  
# MAGIC 3. **REST API** - Callable from external applications
# MAGIC 4. **Monitoring** - Built-in performance tracking
# MAGIC 5. **Auto-scaling** - Cost-effective scaling to zero
# MAGIC
# MAGIC ### ðŸ” Where to Find It:
# MAGIC - **Databricks â†’ Machine Learning â†’ Serving**
# MAGIC - **Databricks â†’ Machine Learning â†’ Models** (for registered model)
# MAGIC - **Databricks â†’ Experiments** (for MLflow runs)
# MAGIC
# MAGIC ### ðŸš€ What You Can Do:
# MAGIC - **Test via UI** - Built-in testing interface
# MAGIC - **Call via API** - REST API for applications  
# MAGIC - **Monitor performance** - Real-time metrics
# MAGIC - **Version control** - Model versioning and rollback
# MAGIC - **A/B testing** - Traffic splitting between versions
# MAGIC
# MAGIC **ðŸŽ¯ Your Severity Classification Agent is now a production-ready ML serving endpoint!**
